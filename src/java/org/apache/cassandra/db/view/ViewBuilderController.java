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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

class ViewBuilderController
{
    private final ColumnFamilyStore baseCfs;
    private final View view;

    /** The local token ranges covered by this builder */
    private final Set<Range<Token>> coveredRanges;

    /** The pending view builders per covered local token range */
    private final Map<Range<Token>, ViewBuilder> pendingBuilders;

    /** The count of keys built by all the token range view builders */
    private volatile long totalBuiltKeys = 0;

    ViewBuilderController(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        coveredRanges = new HashSet<>(StorageService.instance.getLocalRanges(baseCfs.metadata.keyspace));
        pendingBuilders = split(coveredRanges, DatabaseDescriptor.getConcurrentCompactors())
                          .stream().collect(Collectors.toMap(r -> r, r -> new ViewBuilder(baseCfs, view, r, this)));
    }

    /**
     * Starts the view building.
     */
    void start()
    {
        pendingBuilders.values().forEach(CompactionManager.instance::submitViewBuilder);
    }

    /**
     * Stops the view building.
     */
    void stop()
    {
        pendingBuilders.values().forEach(ViewBuilder::stop);
    }

    /**
     * Splits the specified token ranges in at least {@code minParts} token ranges.
     *
     * @param ranges   a collection of token ranges to be split
     * @param minParts the minimum number of returned ranges
     * @return at least {@code minParts} token ranges covering {@code ranges}
     */
    private static Set<Range<Token>> split(Collection<Range<Token>> ranges, int minParts)
    {
        int numRanges = ranges.size();
        if (numRanges >= minParts)
        {
            return new HashSet<>(ranges);
        }
        else
        {
            int partsPerRange = (int) Math.ceil((double) minParts / numRanges);
            return ranges.stream()
                         .map(range -> split(range, partsPerRange))
                         .flatMap(Set::stream)
                         .collect(Collectors.toSet());
        }
    }

    /**
     * Splits the specified token range in {@code parts} subranges.
     *
     * @param range a token range
     * @param parts the number of subranges
     * @return {@code parts} even subranges of {@code range}
     */
    private static Set<Range<Token>> split(Range<Token> range, int parts)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        Set<Range<Token>> subranges = new HashSet<>(parts);
        Token left = range.left;
        for (double i = 1; i <= parts; i++)
        {
            Token right = partitioner.split(range.left, range.right, i / parts);
            subranges.add(new Range<>(left, right));
            left = right;
        }
        return subranges;
    }

    /**
     * Adds any new uncovered token range to the build.
     */
    private void updateCoveredRanges()
    {
        // Find new ranges that are not covered yet
        Collection<Range<Token>> ranges = StorageService.instance.getLocalRanges(baseCfs.metadata.keyspace);
        Set<Range<Token>> newRanges = ranges.stream()
                                            .filter(range -> coveredRanges.stream().noneMatch(r -> r.contains(range)))
                                            .collect(Collectors.toSet());

        // Split the new ranges to satisfy the concurrency factor and run a new view builder for each of them
        int minTasks = DatabaseDescriptor.getConcurrentCompactors() - coveredRanges.size();
        coveredRanges.addAll(newRanges);
        split(newRanges, minTasks).stream()
                                  .map(r -> new ViewBuilder(baseCfs, view, r, this))
                                  .peek(b -> pendingBuilders.put(b.range, b))
                                  .forEach(CompactionManager.instance::submitViewBuilder);
    }

    /**
     * Notifies that the view building for the specified token range has finished after covering the specified number of
     * keys, and returns if this was the last pending range building so the view building can be cosidered as finished.
     *
     * @param range     the token range covered by the finished view builder
     * @param builtKeys the number of keys covered by the finished builder
     * @return {@code true} if this was the last pending range building, {@code false} otherwise
     */
    synchronized boolean notifyFinished(Range<Token> range, long builtKeys)
    {
        updateCoveredRanges();
        totalBuiltKeys += builtKeys;
        pendingBuilders.remove(range);
        return pendingBuilders.isEmpty();
    }

    long keysBuilt()
    {
        return totalBuiltKeys;
    }
}
