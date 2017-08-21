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

package org.apache.cassandra.db.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class ViewBuilderTask extends CompactionInfo.Holder
{
    private static final Logger logger = LoggerFactory.getLogger(ViewBuilderTask.class);

    private static final int ROWS_BETWEEN_CHECKPOINTS = 1000;

    private final ColumnFamilyStore baseCfs;
    private final View view;
    public final Range<Token> range;
    private final ViewBuilder builder;
    private final UUID compactionId;
    private volatile Token prevToken = null;
    private volatile long keysBuilt = 0;
    private volatile boolean isStopped = false;

    public ViewBuilderTask(ColumnFamilyStore baseCfs, View view, Range<Token> range, ViewBuilder builder)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        this.range = range;
        this.builder = builder;
        compactionId = UUIDGen.getTimeUUID();
    }

    private void buildKey(DecoratedKey key)
    {
        ReadQuery selectQuery = view.getReadQuery();

        if (!selectQuery.selectsKey(key))
        {
            logger.trace("Skipping {}, view query filters", key);
            return;
        }

        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = view.getSelectStatement().internalReadForView(key, nowInSec);

        // We're rebuilding everything from what's on disk, so we read everything, consider that as new updates
        // and pretend that there is nothing pre-existing.
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(baseCfs.metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, false);

        try (ReadExecutionController orderGroup = command.executionController();
             UnfilteredRowIterator data = UnfilteredPartitionIterators.getOnlyElement(command.executeLocally(orderGroup), command))
        {
            Iterator<Collection<Mutation>> mutations = baseCfs.keyspace.viewManager
                                                      .forTable(baseCfs.metadata.id)
                                                      .generateViewUpdates(Collections.singleton(view), data, empty, nowInSec, true);

            AtomicLong noBase = new AtomicLong(Long.MAX_VALUE);
            mutations.forEachRemaining(m -> StorageProxy.mutateMV(key.getKey(), m, true, noBase, System.nanoTime()));
        }
    }

    public void run()
    {
        logger.debug("Starting view build for {}.{} for range {}", baseCfs.metadata.keyspace, view.name, range);
        UUID localHostId = SystemKeyspace.getLocalHostId();
        String ksname = baseCfs.metadata.keyspace, viewName = view.name;

        if (SystemKeyspace.isViewBuilt(ksname, viewName))
        {
            logger.debug("View already marked built for {}.{}", baseCfs.metadata.keyspace, view.name);
            if (!SystemKeyspace.isViewStatusReplicated(ksname, viewName))
                updateDistributed(ksname, viewName, localHostId);
            return;
        }

        final Pair<Token, Long> buildStatus = SystemKeyspace.getViewBuildStatus(ksname, viewName, range);
        Token lastToken;
        if (buildStatus == null)
        {
            logger.debug("Starting new view build for range {}. flushing base table {}.{}", range, baseCfs.metadata.keyspace, baseCfs.name);
            lastToken = null;
            SystemKeyspace.beginViewBuild(ksname, viewName, range);
        }
        else
        {
            lastToken = buildStatus.left;
            keysBuilt = buildStatus.right;
            logger.debug("Resuming view build for range {} from token {} with {} covered keys. flushing base table {}.{}",
                         range, lastToken, keysBuilt, baseCfs.metadata.keyspace, baseCfs.name);
        }

        baseCfs.forceBlockingFlush();

        Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>> function;
        function = org.apache.cassandra.db.lifecycle.View.selectFunction(SSTableSet.CANONICAL);

        prevToken = lastToken;

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(function);
             Refs<SSTableReader> sstables = viewFragment.refs;
             ReducingKeyIterator iter = new ReducingKeyIterator(sstables))
        {
            SystemDistributedKeyspace.startViewBuild(ksname, viewName, localHostId);
            while (!isStopped && iter.hasNext())
            {
                DecoratedKey key = iter.next();
                Token token = key.getToken();
                if (lastToken == null || lastToken.compareTo(token) < 0)
                {
                    if (range.contains(token))
                    {
                        buildKey(key);
                        ++keysBuilt;

                        if (prevToken == null || prevToken.compareTo(token) != 0)
                        {
                            if (keysBuilt % ROWS_BETWEEN_CHECKPOINTS == 0)
                                SystemKeyspace.updateViewBuildStatus(ksname, viewName, range, token, keysBuilt);
                            prevToken = token;
                        }
                    }

                    lastToken = null;
                }
            }

            if (!isStopped)
            {
                logger.debug("Completed build of view({}.{}) for range {} after covering {} keys ", ksname, viewName, range, keysBuilt);
                if (builder.notifyFinished(range, keysBuilt))
                {
                    logger.debug("Marking view({}.{}) as built after covering {} keys ", ksname, viewName, builder.builtKeys());
                    SystemKeyspace.finishViewBuildStatus(ksname, viewName);
                    updateDistributed(ksname, viewName, localHostId);
                }
            }
            else
            {
                logger.debug("Stopped build for view({}.{}) for range {} after covering {} keys", ksname, viewName, range, keysBuilt);
            }
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> CompactionManager.instance.submitViewBuilder(this),
                                                         5,
                                                         TimeUnit.MINUTES);
            logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", e);
        }
    }

    private void updateDistributed(String ksname, String viewName, UUID localHostId)
    {
        try
        {
            SystemDistributedKeyspace.successfulViewBuild(ksname, viewName, localHostId);
            SystemKeyspace.setViewBuiltReplicated(ksname, viewName);
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> CompactionManager.instance.submitViewBuilder(this),
                                                         5,
                                                         TimeUnit.MINUTES);
            logger.warn("Failed to updated the distributed status of view, sleeping 5 minutes before retrying", e);
        }
    }

    public CompactionInfo getCompactionInfo()
    {
        long keysTotal = baseCfs.estimatedKeysForRange(range);
        return new CompactionInfo(baseCfs.metadata(), OperationType.VIEW_BUILD, keysBuilt, keysTotal, "ranges", compactionId);
    }

    public void stop()
    {
        isStopped = true;
    }
}
