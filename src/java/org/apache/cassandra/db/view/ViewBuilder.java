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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageService;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Builds a materialized view for the local token ranges.
 * <p>
 * The build is parellelized in at least {@link #concurrencyFactor()} {@link ViewBuilderTask tasks} that are run in the
 * compaction manager.
 */
class ViewBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(ViewBuilderTask.class);

    private final ColumnFamilyStore baseCfs;
    private final View view;
    private final String ksName;
    private final Set<Range<Token>> builtRanges = Sets.newConcurrentHashSet();
    private final Set<ViewBuilderTask> tasks = Sets.newConcurrentHashSet();
    private volatile long keysBuilt = 0;
    private volatile boolean isStopped = false;

    ViewBuilder(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        ksName = baseCfs.metadata.keyspace;

        if (SystemKeyspace.isViewBuilt(ksName, view.name))
        {
            logger.debug("View already marked built for {}.{}", ksName, view.name);
            if (!SystemKeyspace.isViewStatusReplicated(ksName, view.name))
                updateDistributed();
        }
        else
        {
            buildPendingLocalRanges();
        }
    }

    private void buildPendingLocalRanges()
    {
        if (isStopped)
        {
            logger.debug("Stopped build for view({}.{}) after covering {} keys", ksName, view.name, keysBuilt);
            return;
        }

        // Get those local token ranges for which the view hasn't already been built
        Collection<Range<Token>> localRanges = StorageService.instance.getLocalRanges(ksName);
        Set<Range<Token>> ranges = localRanges.stream()
                                              .filter(x -> builtRanges.stream().noneMatch(y -> y.contains(x)))
                                              .collect(toSet());

        // If there are no new ranges we should mark the view as built
        if (ranges.isEmpty())
        {
            logger.debug("Marking view({}.{}) as built after covering {} keys ", ksName, view.name, keysBuilt);
            SystemKeyspace.finishViewBuildStatus(ksName, view.name);
            updateDistributed();
            return;
        }

        // Split the ranges according to the concurrency factor and submit a new view build task for each of them.
        // We record of all the submitted tasks to be able of getting the number of built keys, even if the build is
        // interrupted.
        List<ListenableFuture<Long>> futures;
        futures = DatabaseDescriptor.getPartitioner()
                                    .splitter()
                                    .map(splitter -> splitter.split(ranges, concurrencyFactor()))
                                    .orElse(ranges)
                                    .stream()
                                    .map(range -> new ViewBuilderTask(baseCfs, view, range))
                                    .peek(tasks::add)
                                    .map(CompactionManager.instance::submitViewBuilder)
                                    .collect(toList());

        // Add a callback to process any eventual new local range and mark the view as built, doing a delayed retry if
        // the tasks don't succeed
        ListenableFuture<List<Long>> future = Futures.allAsList(futures);
        Futures.addCallback(future, new FutureCallback<List<Long>>()
        {
            public void onSuccess(List<Long> result)
            {
                keysBuilt += result.stream().mapToLong(x -> x).sum();
                builtRanges.addAll(ranges);
                buildPendingLocalRanges();
            }

            public void onFailure(Throwable t)
            {
                ScheduledExecutors.nonPeriodicTasks.schedule(() -> buildPendingLocalRanges(), 5, TimeUnit.MINUTES);
                logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", t);
            }
        });
    }

    private void updateDistributed()
    {
        try
        {
            UUID localHostId = SystemKeyspace.getLocalHostId();
            SystemDistributedKeyspace.successfulViewBuild(ksName, view.name, localHostId);
            SystemKeyspace.setViewBuiltReplicated(ksName, view.name);
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(this::updateDistributed, 5, TimeUnit.MINUTES);
            logger.warn("Failed to update the distributed status of view, sleeping 5 minutes before retrying", e);
        }
    }

    private static int concurrencyFactor()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    /**
     * Stops the view building.
     */
    synchronized void stop()
    {
        isStopped = true;
        tasks.forEach(ViewBuilderTask::stop);
    }
}
