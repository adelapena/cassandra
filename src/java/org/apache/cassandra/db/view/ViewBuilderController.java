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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static java.util.stream.Collectors.toConcurrentMap;

/**
 * Builds a materialized view for the local token ranges.
 * <p>
 * The build is parellelized in at least {@link #concurrencyFactor()} {@link ViewBuilder} tasks that are run in the
 * {@link CompactionManager}.
 */
class ViewBuilderController
{
    private final ColumnFamilyStore baseCfs;
    private final View view;

    /** The local token ranges covered by this builder */
    private final Set<Range<Token>> ranges;

    /** The pending view builders associated to the start token of their covered token range */
    private final Map<Range<Token>, ViewBuilder> builders;

    /** The count of keys built by all the token range view builders */
    private volatile long builtKeys = 0;

    /** If the view building has been stopped */
    private volatile boolean stopped = false;

    ViewBuilderController(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        ranges = Sets.newConcurrentHashSet(StorageService.instance.getLocalRanges(baseCfs.metadata.keyspace));
        builders = split(ranges, concurrencyFactor())
                   .stream().collect(toConcurrentMap(r -> r, r -> new ViewBuilder(baseCfs, view, r, this)));
        builders.values().forEach(CompactionManager.instance::submitViewBuilder);
    }

    private static int concurrencyFactor()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    private static Set<Range<Token>> split(Set<Range<Token>> ranges, int parts)
    {
        // the partitioner could not have a splitter, in that case we return the ranges unchanged
        return DatabaseDescriptor.getPartitioner()
                                 .splitter()
                                 .map(splitter -> splitter.split(ranges, parts))
                                 .orElse(ranges);
    }

    /**
     * Stops the view building.
     */
    synchronized void stop()
    {
        stopped = true;
        builders.values().forEach(ViewBuilder::stop);
        builders.clear();
        ranges.clear();
    }

    /**
     * Notifies that the view building for the specified token range has finished after covering the specified number of
     * keys, checks if there are any new local ranges to be processed, and returns if this was the last pending range
     * building so the view building can be cosidered as finished.
     *
     * @param range the token range covered by the finished view builder
     * @param builtKeys the number of keys covered by the finished builder
     * @return {@code true} if this was the last pending range build, {@code false} otherwise
     */
    synchronized boolean notifyFinished(Range<Token> range, long builtKeys)
    {
        if (stopped)
            return false;

        this.builtKeys += builtKeys;
        builders.remove(range);

        if (builders.isEmpty())
        {
            // Find any possible new local ranges that has not been considered yet
            Collection<Range<Token>> localRanges = StorageService.instance.getLocalRanges(baseCfs.metadata.keyspace);
            Set<Range<Token>> newRanges = localRanges.stream()
                                                     .filter(x -> ranges.stream().noneMatch(y -> y.contains(x)))
                                                     .collect(Collectors.toSet());

            // If there are no new local ranges we are done
            if (newRanges.isEmpty())
                return true;

            // Split the new ranges to satisfy the concurrency factor and run a new view builder for each of them
            ranges.addAll(newRanges);
            split(newRanges, concurrencyFactor()).stream()
                                                 .map(r -> new ViewBuilder(baseCfs, view, r, this))
                                                 .peek(b -> builders.put(b.range, b))
                                                 .forEach(CompactionManager.instance::submitViewBuilder);
        }
        return false;
    }

    long builtKeys()
    {
        return builtKeys;
    }
}
