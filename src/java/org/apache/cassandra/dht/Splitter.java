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

package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;

import static java.util.stream.Collectors.toSet;

/**
 * Partition splitter.
 */
public abstract class Splitter
{
    private final IPartitioner partitioner;

    protected Splitter(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    protected abstract Token tokenForValue(BigInteger value);

    protected abstract BigInteger valueForToken(Token token);

    public List<Token> splitOwnedRanges(int parts, List<Range<Token>> localRanges, boolean dontSplitRanges)
    {
        if (localRanges.isEmpty() || parts == 1)
            return Collections.singletonList(partitioner.getMaximumToken());

        BigInteger totalTokens = BigInteger.ZERO;
        for (Range<Token> r : localRanges)
        {
            BigInteger right = valueForToken(token(r.right));
            totalTokens = totalTokens.add(right.subtract(valueForToken(r.left)));
        }
        BigInteger perPart = totalTokens.divide(BigInteger.valueOf(parts));
        // the range owned is so tiny we can't split it:
        if (perPart.equals(BigInteger.ZERO))
            return Collections.singletonList(partitioner.getMaximumToken());

        if (dontSplitRanges)
            return splitOwnedRangesNoPartialRanges(localRanges, perPart, parts);

        List<Token> boundaries = new ArrayList<>();
        BigInteger sum = BigInteger.ZERO;
        for (Range<Token> r : localRanges)
        {
            Token right = token(r.right);
            BigInteger currentRangeWidth = valueForToken(right).subtract(valueForToken(r.left)).abs();
            BigInteger left = valueForToken(r.left);
            while (sum.add(currentRangeWidth).compareTo(perPart) >= 0)
            {
                BigInteger withinRangeBoundary = perPart.subtract(sum);
                left = left.add(withinRangeBoundary);
                boundaries.add(tokenForValue(left));
                currentRangeWidth = currentRangeWidth.subtract(withinRangeBoundary);
                sum = BigInteger.ZERO;
            }
            sum = sum.add(currentRangeWidth);
        }
        boundaries.set(boundaries.size() - 1, partitioner.getMaximumToken());

        assert boundaries.size() == parts : boundaries.size() + "!=" + parts + " " + boundaries + ":" + localRanges;
        return boundaries;
    }

    private List<Token> splitOwnedRangesNoPartialRanges(List<Range<Token>> localRanges, BigInteger perPart, int parts)
    {
        List<Token> boundaries = new ArrayList<>(parts);
        BigInteger sum = BigInteger.ZERO;

        int i = 0;
        final int rangesCount = localRanges.size();
        while (boundaries.size() < parts - 1 && i < rangesCount - 1)
        {
            Range<Token> r = localRanges.get(i);
            Range<Token> nextRange = localRanges.get(i + 1);
            Token right = token(r.right);
            Token nextRight = token(nextRange.right);

            BigInteger currentRangeWidth = valueForToken(right).subtract(valueForToken(r.left));
            BigInteger nextRangeWidth = valueForToken(nextRight).subtract(valueForToken(nextRange.left));
            sum = sum.add(currentRangeWidth);

            // does this or next range take us beyond the per part limit?
            if (sum.compareTo(perPart) > 0 || sum.add(nextRangeWidth).compareTo(perPart) > 0)
            {
                // Either this or the next range will take us beyond the perPart limit. Will stopping now or
                // adding the next range create the smallest difference to perPart?
                BigInteger diffCurrent = sum.subtract(perPart).abs();
                BigInteger diffNext = sum.add(nextRangeWidth).subtract(perPart).abs();
                if (diffNext.compareTo(diffCurrent) >= 0)
                {
                    sum = BigInteger.ZERO;
                    boundaries.add(right);
                }
            }
            i++;
        }
        boundaries.add(partitioner.getMaximumToken());
        return boundaries;
    }

    /**
     * We avoid calculating for wrap around ranges, instead we use the actual max token, and then, when translating
     * to PartitionPositions, we include tokens from .minKeyBound to .maxKeyBound to make sure we include all tokens.
     */
    private Token token(Token t)
    {
        return t.equals(partitioner.getMinimumToken()) ? partitioner.getMaximumToken() : t;
    }

    /**
     * Splits the specified token ranges in at least {@code parts} subranges.
     * <p>
     * Each returned subrange will be contained in exactly one of the specified ranges.
     *
     * @param ranges a collection of token ranges to be split
     * @param parts the minimum number of returned ranges
     * @return at least {@code minParts} token ranges covering {@code ranges}
     */
    public Set<Range<Token>> split(Collection<Range<Token>> ranges, int parts)
    {
        int numRanges = ranges.size();
        if (numRanges >= parts)
        {
            return Sets.newHashSet(ranges);
        }
        else
        {
            int partsPerRange = (int) Math.ceil((double) parts / numRanges);
            return ranges.stream()
                         .map(range -> split(range, partsPerRange))
                         .flatMap(Collection::stream)
                         .collect(toSet());
        }
    }

    /**
     * Splits the specified token range in at least {@code minParts} subranges, unless the range has not enough tokens
     * in which case the range will be returned without splitting.
     *
     * @param range a token range
     * @param parts the number of subranges
     * @return {@code parts} even subranges of {@code range}
     */
    private Set<Range<Token>> split(Range<Token> range, int parts)
    {
        // the range might not have enough tokens to split
        BigInteger numTokens = valueForToken(token(range.right)).subtract(valueForToken(range.left)).abs();
        if (BigInteger.valueOf(parts).compareTo(numTokens) > 0)
            return Collections.singleton(range);

        Token left = range.left;
        Set<Range<Token>> subranges = new HashSet<>(parts);
        for (double i = 1; i <= parts; i++)
        {
            Token right = partitioner.split(range.left, range.right, i / parts);
            subranges.add(new Range<>(left, right));
            left = right;
        }
        return subranges;
    }
}
