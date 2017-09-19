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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
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

public class ViewBuilderTask extends CompactionInfo.Holder implements Callable<Long>
{
    private static final Logger logger = LoggerFactory.getLogger(ViewBuilderTask.class);

    private static final int ROWS_BETWEEN_CHECKPOINTS = 1000;

    private final ColumnFamilyStore baseCfs;
    private final View view;
    public final Range<Token> range;
    private volatile long keysBuilt = 0;
    private volatile boolean isStopped = false;
    private final UUID compactionId;
    private volatile Token prevToken;

    ViewBuilderTask(ColumnFamilyStore baseCfs, View view, Range<Token> range)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        this.range = range;
        this.compactionId = UUIDGen.getTimeUUID();
        this.prevToken = null;
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

    public Long call()
    {
        logger.debug("Starting view build for {}.{} for range {}", baseCfs.metadata.keyspace, view.name, range);
        UUID localHostId = SystemKeyspace.getLocalHostId();
        String ksname = baseCfs.metadata.keyspace, viewName = view.name;

        final Pair<Token, Long> buildStatus = SystemKeyspace.getViewBuildStatus(ksname, viewName, range);
        if (buildStatus == null || buildStatus.right == 0)
        {
            logger.debug("Starting new view build for range {}. flushing base table {}.{}", range, baseCfs.metadata.keyspace, baseCfs.name);
            SystemKeyspace.beginViewBuild(ksname, viewName, range);
        }
        else
        {
            prevToken = buildStatus.left;
            keysBuilt = buildStatus.right;
            logger.debug("Resuming view build for range {} from token {} with {} covered keys. flushing base table {}.{}",
                         range, prevToken, keysBuilt, baseCfs.metadata.keyspace, baseCfs.name);
        }

        baseCfs.forceBlockingFlush();

        Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>> function;
        function = org.apache.cassandra.db.lifecycle.View.select(SSTableSet.CANONICAL, s -> range.intersects(s.getBounds()));

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(function);
             Refs<SSTableReader> sstables = viewFragment.refs;
             ReducingKeyIterator keyIter = new ReducingKeyIterator(sstables))
        {
            SystemDistributedKeyspace.startViewBuild(ksname, viewName, localHostId);

            PeekingIterator<DecoratedKey> iter = Iterators.peekingIterator(keyIter);
            while (!isStopped && iter.hasNext())
            {
                DecoratedKey key = iter.next();
                Token token = key.getToken();
                //skip tokens already built or not present in range
                if (range.contains(token) && (prevToken == null || token.compareTo(prevToken) > 0))
                {
                    buildKey(key);
                    ++keysBuilt;
                    //build other keys sharing the same token
                    while (iter.hasNext() && iter.peek().getToken().equals(token))
                    {
                        key = iter.next();
                        buildKey(key);
                        ++keysBuilt;
                    }
                    if (keysBuilt % ROWS_BETWEEN_CHECKPOINTS == 1)
                        SystemKeyspace.updateViewBuildStatus(ksname, viewName, range, token, keysBuilt);
                    prevToken = token;
                }
            }

            if (!isStopped)
                logger.debug("Completed build of view({}.{}) for range {} after covering {} keys ", ksname, viewName, range, keysBuilt);
            else
                logger.debug("Stopped build for view({}.{}) for range {} after covering {} keys", ksname, viewName, range, keysBuilt);
        }

        return keysBuilt;
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        // If there's splitter, calculate progress based on last token position
        if (range.left.getPartitioner().splitter().isPresent())
        {
            long progress = prevToken == null? 0 : Math.round(prevToken.getPartitioner().splitter().get().positionInRange(prevToken, range) * 1000);
            return new CompactionInfo(baseCfs.metadata(), OperationType.VIEW_BUILD, progress, 1000, "token range parts", compactionId);
        }

        // When there is no splitter, estimate based on number of total keys but
        // take the max with keysBuilt + 1 to avoid having more completed than total
        long keysTotal = Math.max(keysBuilt + 1, baseCfs.estimatedKeysForRange(range));
        return new CompactionInfo(baseCfs.metadata(), OperationType.VIEW_BUILD, keysBuilt, keysTotal, "keys", compactionId);
    }

    @Override
    public void stop()
    {
        isStopped = true;
    }
}
