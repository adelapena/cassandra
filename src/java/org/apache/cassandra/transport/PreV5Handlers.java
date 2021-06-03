/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.transport;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ClientResourceLimits.Overload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.transport.ClientResourceLimits.CONNECTION_WAKER;
import static org.apache.cassandra.transport.ClientResourceLimits.GLOBAL_REQUEST_LIMITER;

public class PreV5Handlers
{
    /**
     * Wraps an {@link org.apache.cassandra.transport.Dispatcher} so that it can be used as an
     * channel inbound handler in pre-V5 pipelines.
     */
    public static class LegacyDispatchHandler extends SimpleChannelInboundHandler<Message.Request>
    {
        private static final Logger logger = LoggerFactory.getLogger(LegacyDispatchHandler.class);

        private final Dispatcher dispatcher;
        private final ClientResourceLimits.Allocator endpointPayloadTracker;

        /**
         * Current count of *request* bytes that are live on the channel.
         * <p>
         * Note: should only be accessed while on the netty event loop.
         */
        private long channelPayloadBytesInFlight;

        LegacyDispatchHandler(Dispatcher dispatcher, ClientResourceLimits.Allocator endpointPayloadTracker)
        {
            this.dispatcher = dispatcher;
            this.endpointPayloadTracker = endpointPayloadTracker;
        }

        protected void channelRead0(ChannelHandlerContext ctx, Message.Request request)
        {
            // The only reason we won't process this message is if we throw an OverloadedException.
            Overload backpressure = checkLimits(ctx, request);
            dispatcher.dispatch(ctx.channel(), request, this::toFlushItem, backpressure);
        }

        // Acts as a Dispatcher.FlushItemConverter
        private Flusher.FlushItem.Unframed toFlushItem(Channel channel, Message.Request request, Message.Response response)
        {
            return new Flusher.FlushItem.Unframed(channel, response, request.getSource(), this::releaseItem);
        }

        private void releaseItem(Flusher.FlushItem<Message.Response> item)
        {
            // Note: in contrast to the equivalent for V5 protocol, CQLMessageHandler::release(FlushItem item),
            // this does not release the FlushItem's Message.Response. In V4, the buffers for the response's body
            // and serialised header are emitted directly down the Netty pipeline from Envelope.Encoder, so
            // releasing them is handled by the pipeline itself.
            long itemSize = item.request.header.bodySizeInBytes;
            item.request.release();

            // since the request has been processed, decrement inflight payload at channel, endpoint and global levels
            channelPayloadBytesInFlight -= itemSize;
            boolean globalInFlightBytesBelowLimit = endpointPayloadTracker.release(itemSize) == ResourceLimits.Outcome.BELOW_LIMIT;

            // Now check to see if we need to reenable the channel's autoRead.
            //
            // If the current payload bytes in flight is zero, we must reenable autoread as
            // 1) we allow no other thread/channel to do it, and
            // 2) there are no other events following this one (becuase we're at zero bytes in flight),
            // so no successive to trigger the other clause in this if-block.
            //
            // The only exception to this is if the global request rate limit has been breached, which means
            // we'll have to wait until a scheduled wakeup task unpauses the connection.
            //
            // note: this path is only relevant when part of a pre-V5 pipeline, as only in this case is
            // paused ever set to true. In pipelines configured for V5 or later, backpressure and control
            // over the inbound pipeline's autoread status are handled by the FrameDecoder/FrameProcessor.
            ChannelConfig config = item.channel.config();

            if (!config.isAutoRead() && (channelPayloadBytesInFlight == 0 || globalInFlightBytesBelowLimit) && GLOBAL_REQUEST_LIMITER.canAcquire())
            {
                unpauseConnection(config);
            }
        }

        /**
         * This check for inflight payload to potentially discard the request should have been ideally in one of the
         * first handlers in the pipeline (Envelope.Decoder::decode()). However, incase of any exception thrown between that
         * handler (where inflight payload is incremented) and this handler (Dispatcher::channelRead0) (where inflight
         * payload in decremented), inflight payload becomes erroneous. ExceptionHandler is not sufficient for this
         * purpose since it does not have the message envelope associated with the exception.
         * <p>
         * Note: this method should execute on the netty event loop.
         * 
         * @return the type of {@link Overload} triggered
         */
        @SuppressWarnings("UnstableApiUsage")
        private Overload checkLimits(ChannelHandlerContext ctx, Message.Request request)
        {
            long requestSize = request.getSource().header.bodySizeInBytes;
            Overload backpressure = Overload.NONE;

            // check for overloaded state by trying to allocate the message size from inflight payload trackers
            if (endpointPayloadTracker.tryAllocate(requestSize) != ResourceLimits.Outcome.SUCCESS)
            {
                if (request.connection.isThrowOnOverload())
                {
                    discardAndThrow(request, requestSize, Overload.BYTES_IN_FLIGHT);
                }
                else
                {
                    // set backpressure on the channel, and handle the request
                    endpointPayloadTracker.allocate(requestSize);
                    pauseConnection(ctx);
                    backpressure = Overload.BYTES_IN_FLIGHT;
                }
            }

            if (DatabaseDescriptor.getNativeTransportRateLimitingEnabled()) 
            {
                // Make sure we haven't breached the configured request-level rate limit.
                if (request.connection.isThrowOnOverload()) 
                {
                    if (!GLOBAL_REQUEST_LIMITER.tryAcquire())
                    {
                        // We've already allocated against the payload tracker here, so release those 
                        // resources before aborting.
                        endpointPayloadTracker.release(requestSize);
                        discardAndThrow(request, requestSize, Overload.REQUESTS);
                    }
                } 
                else if (GLOBAL_REQUEST_LIMITER.reserveAndGetWaitLength() > 0)
                {
                    NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 1, TimeUnit.MINUTES,
                                     "Request breached global limit of {} requests/second and triggered backpressure.",
                                     GLOBAL_REQUEST_LIMITER.getRate());
                    
                    // Force acquire a permit and handle the request, but set backpressure on the channel first.
                    backpressure = Overload.REQUESTS;
                    pauseConnection(ctx);

                    CONNECTION_WAKER.submit(() ->
                    {
                        // We should be able to unpause if either rate limiting is disabled or a permits is available.
                        while (DatabaseDescriptor.getNativeTransportRateLimitingEnabled() && !GLOBAL_REQUEST_LIMITER.canAcquire()) 
                        {
                            Uninterruptibles.sleepUninterruptibly(GLOBAL_REQUEST_LIMITER.waitTimeMicros(), TimeUnit.MICROSECONDS);
                        }

                        ctx.channel().eventLoop().execute(() -> unpauseConnection(ctx.channel().config()));
                    });
                }
            }

            channelPayloadBytesInFlight += requestSize;
            return backpressure;
        }

        private void pauseConnection(ChannelHandlerContext ctx)
        {
            if (ctx.channel().config().isAutoRead())
            {
                ctx.channel().config().setAutoRead(false);
                ClientMetrics.instance.pauseConnection();
            }
        }

        private void unpauseConnection(ChannelConfig config)
        {
            if (!config.isAutoRead())
            {
                ClientMetrics.instance.unpauseConnection();
                config.setAutoRead(true);
            }
        }

        private void discardAndThrow(Message.Request request, long requestSize, Overload overload)
        {
            ClientMetrics.instance.markRequestDiscarded();

            logger.trace("Discarded request of size {} with {} bytes in flight on channel. {} " + 
                         "Global rate limiter: {} Request: {}",
                         requestSize, channelPayloadBytesInFlight, endpointPayloadTracker,
                         GLOBAL_REQUEST_LIMITER, request);

            OverloadedException exception = overload == Overload.REQUESTS
                    ? new OverloadedException(String.format("Request breached global limit of %f requests/second. Server is " +
                                                            "currently in an overloaded state and cannot accept more requests.",
                                                            GLOBAL_REQUEST_LIMITER.getRate()))
                    : new OverloadedException(String.format("Request breached limit on bytes in flight. (%s)) " +
                                                            "Server is currently in an overloaded state and cannot accept more requests.",

                    endpointPayloadTracker));
            
            throw ErrorMessage.wrap(exception, request.getSource().header.streamId);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx)
        {
            endpointPayloadTracker.release();
            if (!ctx.channel().config().isAutoRead())
            {
                ClientMetrics.instance.unpauseConnection();
            }
            ctx.fireChannelInactive();
        }
    }

    /**
     * Simple adaptor to allow {@link org.apache.cassandra.transport.Message.Decoder#decodeMessage(Channel, Envelope)}
     * to be used as a handler in pre-V5 pipelines
     */
    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Envelope>
    {
        public static final ProtocolDecoder instance = new ProtocolDecoder();
        private ProtocolDecoder(){}

        public void decode(ChannelHandlerContext ctx, Envelope source, List<Object> results)
        {
            try
            {
                ProtocolVersion version = getConnectionVersion(ctx);
                if (source.header.version != version)
                {
                    throw new ProtocolException(
                        String.format("Invalid message version. Got %s but previous " +
                                      "messages on this connection had version %s",
                                      source.header.version, version));
                }
                results.add(Message.Decoder.decodeMessage(ctx.channel(), source));
            }
            catch (Throwable ex)
            {
                source.release();
                // Remember the streamId
                throw ErrorMessage.wrap(ex, source.header.streamId);
            }
        }
    }

    /**
     * Simple adaptor to plug CQL message encoding into pre-V5 pipelines
     */
    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message>
    {
        public static final ProtocolEncoder instance = new ProtocolEncoder();
        private ProtocolEncoder(){}
        public void encode(ChannelHandlerContext ctx, Message source, List<Object> results)
        {
            ProtocolVersion version = getConnectionVersion(ctx);
            results.add(source.encode(version));
        }
    }

    /**
     * Pre-V5 exception handler which closes the connection if an {@link org.apache.cassandra.transport.ProtocolException}
     * is thrown
     */
    @ChannelHandler.Sharable
    public static final class ExceptionHandler extends ChannelInboundHandlerAdapter
    {
        public static final ExceptionHandler instance = new ExceptionHandler();
        private ExceptionHandler(){}

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        {
            // Provide error message to client in case channel is still open
            if (ctx.channel().isOpen())
            {
                ExceptionHandlers.UnexpectedChannelExceptionHandler handler = new ExceptionHandlers.UnexpectedChannelExceptionHandler(ctx.channel(), false);
                ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
                ChannelFuture future = ctx.writeAndFlush(errorMessage.encode(getConnectionVersion(ctx)));
                // On protocol exception, close the channel as soon as the message have been sent.
                // Most cases of PE are wrapped so the type check below is expected to fail more often than not.
                // At this moment Fatal exceptions are not thrown in v4, but just as a precaustion we check for them here
                if (isFatal(cause))
                    future.addListener((ChannelFutureListener) f -> ctx.close());
            }
            
            ExceptionHandlers.logClientNetworkingExceptions(cause);
            JVMStabilityInspector.inspectThrowable(cause);
        }

        private static boolean isFatal(Throwable cause)
        {
            return cause instanceof ProtocolException; // this matches previous versions which didn't annotate exceptions as fatal or not
        }
    }

    private static ProtocolVersion getConnectionVersion(ChannelHandlerContext ctx)
    {
        Connection connection = ctx.channel().attr(Connection.attributeKey).get();
        // The only case the connection can be null is when we send the initial STARTUP message
        return connection == null ? ProtocolVersion.CURRENT : connection.getVersion();
    }

}
