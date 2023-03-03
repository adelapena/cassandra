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
package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.tries.SerializationNode;
import org.apache.cassandra.io.tries.TrieNode;
import org.apache.cassandra.io.tries.TrieSerializer;
import org.apache.cassandra.io.tries.Walker;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SizedInts;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.ArrayUtil;

/**
 * Page-aware random access reader for a trie terms dictionary written by {@link TrieTermsDictionaryWriter}.
 */
@NotThreadSafe
public class TrieTermsDictionaryReader extends Walker<TrieTermsDictionaryReader>
{
    public static final long NOT_FOUND = -1;

    public TrieTermsDictionaryReader(Rebufferer rebufferer, long root)
    {
        super(rebufferer, root);
    }

    public static final TrieSerializer<Long, DataOutputPlus> trieSerializer = new TrieSerializer<Long, DataOutputPlus>()
    {
        @Override
        public int sizeofNode(SerializationNode<Long> node, long nodePosition)
        {
            return TrieNode.typeFor(node, nodePosition).sizeofNode(node) + sizeof(node.payload());
        }

        @Override
        public void write(DataOutputPlus dest, SerializationNode<Long> node, long nodePosition) throws IOException
        {
            TrieNode type = TrieNode.typeFor(node, nodePosition);
            Long payload = node.payload();
            int payloadBits = sizeof(payload);
            type.serialize(dest, node, payloadBits, nodePosition);

            if (payload != null)
                SizedInts.write(dest, payload, payloadBits);
        }

        private int sizeof(Long payload)
        {
            return payload == null ? 0 : SizedInts.nonZeroSize(payload);
        }
    };

    public long exactMatch(ByteComparable key)
    {
        // Since we are looking for an exact match we are always expecting the follow
        // to return END_OF_STREAM if the key was found.
        return follow(key) == ByteSource.END_OF_STREAM ? getCurrentPayload() : NOT_FOUND;
    }

    public Iterator<Pair<ByteComparable, Long>> iterator()
    {
        return new AbstractIterator<Pair<ByteComparable, Long>>()
        {
            final TransitionBytesCollector collector = new TransitionBytesCollector();
            IterationPosition stack = new IterationPosition(root, null);

            @Override
            protected Pair<ByteComparable, Long> computeNext()
            {
                return advanceNode() == NOT_FOUND ? endOfData()
                                                  : Pair.create(collector.toByteComparable(), getCurrentPayload());
            }

            private long advanceNode()
            {
                long child;
                int transitionByte;

                go(stack.node);
                while (true)
                {
                    int childIndex = stack.childIndex + 1;
                    transitionByte = transitionByte(childIndex);

                    if (transitionByte == Integer.MAX_VALUE)
                    {
                        // ascend
                        stack = stack.prev;
                        collector.pop();
                        if (stack == null)
                        {
                            // exhausted whole trie
                            return NOT_FOUND;
                        }
                        go(stack.node);
                        continue;
                    }

                    child = transition(childIndex);
                    stack.childIndex = childIndex;

                    if (child != -1)
                    {
                        assert child >= 0 : String.format("Expected value >= 0 but got %d - %s", child, this);

                        // descend
                        go(child);

                        stack = new IterationPosition(child, stack);
                        collector.add(transitionByte);

                        if (hasPayload())
                            return child;
                    }
                }
            }
        };
    }

    public ByteComparable getMaxTerm()
    {
        TransitionBytesCollector collector = new ImmutableTransitionBytesCollector();
        go(root);
        while (true)
        {
            int lastIdx = transitionRange() - 1;
            long lastChild = transition(lastIdx);
            if (lastIdx < 0)
            {
                return collector.toByteComparable();
            }
            collector.add(transitionByte(lastIdx));
            go(lastChild);
        }
    }

    public ByteComparable getMinTerm()
    {
        TransitionBytesCollector collector = new ImmutableTransitionBytesCollector();
        go(root);
        while (true)
        {
            if (hasPayload())
            {
                return collector.toByteComparable();
            }
            collector.add(transitionByte(0));
            go(transition(0));
        }
    }

    private long getCurrentPayload()
    {
        return getPayload(buf, payloadPosition(), payloadFlags());
    }

    private long getPayload(ByteBuffer contents, int payloadPos, int bytes)
    {
        if (bytes == 0)
        {
            return NOT_FOUND;
        }
        return SizedInts.read(contents, payloadPos, bytes);
    }

    public static class ImmutableTransitionBytesCollector extends TransitionBytesCollector
    {
        @Override
        public ByteComparable toByteComparable()
        {
            assert pos > 0 : "Cannot create a byte comparable from an empty value";
            int length = pos;
            return v -> ByteSource.fixedLength(bytes, 0, length);
        }

        @Override
        public void pop()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class TransitionBytesCollector
    {
        protected byte[] bytes = new byte[32];
        protected int pos = 0;

        public void add(int b)
        {
            if (pos == bytes.length)
            {
                bytes = ArrayUtil.grow(bytes, pos + 1);
            }
            bytes[pos++] = (byte) b;
        }

        public void pop()
        {
            assert pos >= 0;
            pos--;
        }

        public ByteComparable toByteComparable()
        {
            assert pos > 0 : "Cannot create a byte comparable from an empty value";
            byte[] value = new byte[pos];
            System.arraycopy(bytes, 0, value, 0, pos);
            return v -> ByteSource.fixedLength(value, 0, value.length);
        }

        public void reset()
        {
            pos = 0;
        }

        @Override
        public String toString()
        {
            return String.format("[Bytes %s, pos %d]", Arrays.toString(bytes), pos);
        }
    }

    private static class IterationPosition
    {
        final long node;
        final IterationPosition prev;
        int childIndex;

        IterationPosition(long node, IterationPosition prev)
        {
            this.node = node;
            this.childIndex = -1;
            this.prev = prev;
        }

        @Override
        public String toString()
        {
            return String.format("[Node %d, child %d]", node, childIndex);
        }
    }
}
