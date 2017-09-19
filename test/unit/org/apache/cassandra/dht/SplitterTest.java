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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SplitterTest
{

    @Test
    public void randomSplitTestNoVNodesRandomPartitioner()
    {
        randomSplitTestNoVNodes(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestNoVNodesMurmur3Partitioner()
    {
        randomSplitTestNoVNodes(new Murmur3Partitioner());
    }

    @Test
    public void randomSplitTestVNodesRandomPartitioner()
    {
        randomSplitTestVNodes(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestVNodesMurmur3Partitioner()
    {
        randomSplitTestVNodes(new Murmur3Partitioner());
    }

    public void randomSplitTestNoVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            List<Range<Token>> localRanges = generateLocalRanges(1, r.nextInt(4) + 1, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(r.nextInt(9) + 1, localRanges, false);
            assertTrue("boundaries = " + boundaries + " ranges = " + localRanges, assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, true));
        }
    }

    public void randomSplitTestVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            // we need many tokens to be able to split evenly over the disks
            int numTokens = 172 + r.nextInt(128);
            int rf = r.nextInt(4) + 2;
            int parts = r.nextInt(5) + 1;
            List<Range<Token>> localRanges = generateLocalRanges(numTokens, rf, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, true);
            if (!assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, false))
                fail(String.format("Could not split %d tokens with rf=%d into %d parts (localRanges=%s, boundaries=%s)", numTokens, rf, parts, localRanges, boundaries));
        }
    }

    private boolean assertRangeSizeEqual(List<Range<Token>> localRanges, List<Token> tokens, IPartitioner partitioner, Splitter splitter, boolean splitIndividualRanges)
    {
        Token start = partitioner.getMinimumToken();
        List<BigInteger> splits = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++)
        {
            Token end = i == tokens.size() - 1 ? partitioner.getMaximumToken() : tokens.get(i);
            splits.add(sumOwnedBetween(localRanges, start, end, splitter, splitIndividualRanges));
            start = end;
        }
        // when we dont need to keep around full ranges, the difference is small between the partitions
        BigDecimal delta = splitIndividualRanges ? BigDecimal.valueOf(0.001) : BigDecimal.valueOf(0.2);
        boolean allBalanced = true;
        for (BigInteger b : splits)
        {
            for (BigInteger i : splits)
            {
                BigDecimal bdb = new BigDecimal(b);
                BigDecimal bdi = new BigDecimal(i);
                BigDecimal q = bdb.divide(bdi, 2, BigDecimal.ROUND_HALF_DOWN);
                if (q.compareTo(BigDecimal.ONE.add(delta)) > 0 || q.compareTo(BigDecimal.ONE.subtract(delta)) < 0)
                    allBalanced = false;
            }
        }
        return allBalanced;
    }

    private BigInteger sumOwnedBetween(List<Range<Token>> localRanges, Token start, Token end, Splitter splitter, boolean splitIndividualRanges)
    {
        BigInteger sum = BigInteger.ZERO;
        for (Range<Token> range : localRanges)
        {
            if (splitIndividualRanges)
            {
                Set<Range<Token>> intersections = new Range<>(start, end).intersectionWith(range);
                for (Range<Token> intersection : intersections)
                    sum = sum.add(splitter.valueForToken(intersection.right).subtract(splitter.valueForToken(intersection.left)));
            }
            else
            {
                if (new Range<>(start, end).contains(range.left))
                    sum = sum.add(splitter.valueForToken(range.right).subtract(splitter.valueForToken(range.left)));
            }
        }
        return sum;
    }

    private List<Range<Token>> generateLocalRanges(int numTokens, int rf, Splitter splitter, Random r, boolean randomPartitioner)
    {
        int localTokens = numTokens * rf;
        List<Token> randomTokens = new ArrayList<>();

        for (int i = 0; i < localTokens * 2; i++)
        {
            Token t = splitter.tokenForValue(randomPartitioner ? new BigInteger(127, r) : BigInteger.valueOf(r.nextLong()));
            randomTokens.add(t);
        }

        Collections.sort(randomTokens);

        List<Range<Token>> localRanges = new ArrayList<>(localTokens);
        for (int i = 0; i < randomTokens.size() - 1; i++)
        {
            assert randomTokens.get(i).compareTo(randomTokens.get(i + 1)) < 0;
            localRanges.add(new Range<>(randomTokens.get(i), randomTokens.get(i + 1)));
            i++;
        }
        return localRanges;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSplit()
    {
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;

        // regular single range
        testSplit(1, newHashSet(range(1, 100)), newHashSet(range(1, 100)));
        testSplit(2, newHashSet(range(1, 100)), newHashSet(range(1, 50), range(50, 100)));
        testSplit(4, newHashSet(range(1, 100)), newHashSet(range(1, 25), range(25, 50), range(50, 75), range(75, 100)));

        // single range too small to be partitioned
        testSplit(1, newHashSet(range(1, 2)), newHashSet(range(1, 2)));
        testSplit(2, newHashSet(range(1, 2)), newHashSet(range(1, 2)));
        testSplit(4, newHashSet(range(1, 4)), newHashSet(range(1, 4)));
        testSplit(8, newHashSet(range(1, 2)), newHashSet(range(1, 2)));

        // single wrapping range
        testSplit(2, newHashSet(range(4, -4)), newHashSet(range(4, max), range(max, -4)));
        testSplit(2, newHashSet(range(2, -6)), newHashSet(range(2, max - 2), range(max - 2, -6)));
        testSplit(2, newHashSet(range(6, -4)), newHashSet(range(6, min + 1), range(min + 1, -4)));
        testSplit(2, newHashSet(range(1, 0)), newHashSet(range(min + 1, 0), range(1, min + 1)));

        // single range around partitioner min/max values
        testSplit(2, newHashSet(range(max - 8, min)), newHashSet(range(max - 8, max - 4), range(max - 4, max)));
        testSplit(2, newHashSet(range(max - 8, max)), newHashSet(range(max - 8, max - 4), range(max - 4, max)));
        testSplit(2, newHashSet(range(min, min + 8)), newHashSet(range(min, min + 4), range(min + 4, min + 8)));
        testSplit(2, newHashSet(range(max, min + 8)), newHashSet(range(max, min + 4), range(min + 4, min + 8)));
        testSplit(2, newHashSet(range(max - 4, min + 4)), newHashSet(range(max - 4, max), range(max, min + 4)));
        testSplit(2, newHashSet(range(max - 4, min + 8)), newHashSet(range(max - 4, min + 2), range(min + 2, min + 8)));

        // multiple ranges
        testSplit(1, newHashSet(range(1, 100), range(200, 300)), newHashSet(range(1, 100), range(200, 300)));
        testSplit(2, newHashSet(range(1, 100), range(200, 300)), newHashSet(range(1, 100), range(200, 300)));
        testSplit(4,
                  newHashSet(range(1, 100), range(200, 300)),
                  newHashSet(range(1, 50), range(50, 100), range(200, 250), range(250, 300)));
        testSplit(4,
                  newHashSet(range(1, 100), range(200, 300), range(max - 4, min + 4)),
                  newHashSet(range(1, 50),
                             range(50, 100),
                             range(200, 250),
                             range(250, 300),
                             range(max, min + 4),
                             range(max - 4, max)));
    }

    private static void testSplit(int parts, Set<Range<Token>> ranges, Set<Range<Token>> expected)
    {
        Splitter splitter = new Murmur3Partitioner().splitter().get();
        Set<Range<Token>> actual = splitter.split(ranges, parts);
        assertEquals(expected, actual);
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(token(left), token(right));
    }

    private static Token token(long n)
    {
        return new Murmur3Partitioner.LongToken(n);
    }

    @Test
    public void testTokensInRangeRandomPartitioner()
    {
        testTokensInRange(new RandomPartitioner());
    }

    @Test
    public void testTokensInRangeMurmur3Partitioner()
    {
        testTokensInRange(new Murmur3Partitioner());
    }

    public void testTokensInRange(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();

        // test full range
        Range<Token> fullRange = new Range(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        BigInteger fullRangeSize = splitter.valueForToken(partitioner.getMaximumToken()).subtract(splitter.valueForToken(partitioner.getMinimumToken()));
        assertEquals(fullRangeSize, splitter.tokensInRange(fullRange));
        fullRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-10)), splitter.tokenForValue(BigInteger.valueOf(-10)));
        assertEquals(fullRangeSize, splitter.tokensInRange(fullRange));

        // test small range
        Range<Token> smallRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-5)), splitter.tokenForValue(BigInteger.valueOf(5)));
        assertEquals(BigInteger.valueOf(10), splitter.tokensInRange(smallRange));

        // test wrap-around range
        Range<Token> wrapAround = new Range<>(splitter.tokenForValue(BigInteger.valueOf(5)), splitter.tokenForValue(BigInteger.valueOf(-5)));
        assertEquals(fullRangeSize.subtract(BigInteger.TEN), splitter.tokensInRange(wrapAround));
    }

    @Test
    public void testElapsedTokensRandomPartitioner()
    {
        testElapsedMultiRange(new RandomPartitioner());
    }

    @Test
    public void testElapsedTokensMurmur3Partitioner()
    {
        testElapsedMultiRange(new Murmur3Partitioner());
    }

    public void testElapsedMultiRange(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        // small range
        Range<Token> smallRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-1)), splitter.tokenForValue(BigInteger.valueOf(1)));
        testElapsedTokens(partitioner, smallRange, true);

        // medium range
        Range<Token> mediumRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(0)), splitter.tokenForValue(BigInteger.valueOf(123456789)));
        testElapsedTokens(partitioner, mediumRange, true);

        // wrapped range
        BigInteger min = splitter.valueForToken(partitioner.getMinimumToken());
        BigInteger max = splitter.valueForToken(partitioner.getMaximumToken());
        Range<Token> wrappedRange = new Range<>(splitter.tokenForValue(max.subtract(BigInteger.valueOf(1350))),
                                         splitter.tokenForValue(min.add(BigInteger.valueOf(20394))));
        testElapsedTokens(partitioner, wrappedRange, true);

        // full range
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        testElapsedTokens(partitioner, fullRange, false);
    }

    public void testElapsedTokens(IPartitioner partitioner, Range<Token> range, boolean partialRange)
    {
        Splitter splitter = partitioner.splitter().get();

        BigInteger left = splitter.valueForToken(range.left);
        BigInteger right = splitter.valueForToken(range.right);
        BigInteger tokensInRange = splitter.tokensInRange(range);

        // elapsedTokens(left, (left, right]) = 0
        assertEquals(BigInteger.ZERO, splitter.elapsedTokens(splitter.tokenForValue(left), range));

        // elapsedTokens(right, (left, right]) = tokensInRange((left, right])
        assertEquals(tokensInRange, splitter.elapsedTokens(splitter.tokenForValue(right), range));

        // elapsedTokens(left+1, (left, right]) = 1
        assertEquals(BigInteger.ONE, splitter.elapsedTokens(splitter.tokenForValue(left.add(BigInteger.ONE)), range));

        // elapsedTokens(right-1, (left, right]) = tokensInRange((left, right]) - 1
        assertEquals(tokensInRange.subtract(BigInteger.ONE), splitter.elapsedTokens(splitter.tokenForValue(right.subtract(BigInteger.ONE)), range));

        // elapsedTokens(midpoint, (left, right]) + tokensInRange((midpoint, right]) = tokensInRange
        Token midpoint = partitioner.midpoint(range.left, range.right);
        assertEquals(tokensInRange, splitter.elapsedTokens(midpoint, range).add(splitter.tokensInRange(new Range<>(midpoint, range.right))));

        if (partialRange)
        {
            // elapsedTokens(right + 1, (left, right]) = 0
            assertEquals(BigInteger.ZERO, splitter.elapsedTokens(splitter.tokenForValue(right.add(BigInteger.ONE)), range));
        }
    }

    @Test
    public void testPositionInRangeRandomPartitioner()
    {
        testPositionInRangeMultiRange(new RandomPartitioner());
    }

    @Test
    public void testPositionInRangeMurmur3Partitioner()
    {
        testPositionInRangeMultiRange(new Murmur3Partitioner());
    }

    public void testPositionInRangeMultiRange(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();

        // Test tiny range
        Token start = splitter.tokenForValue(BigInteger.ZERO);
        Token end = splitter.tokenForValue(BigInteger.valueOf(3));
        Range range = new Range<>(start, end);
        assertEquals(0.0, splitter.positionInRange(start, range), 0.01);
        assertEquals(0.33, splitter.positionInRange(splitter.tokenForValue(BigInteger.valueOf(1)), range), 0.01);
        assertEquals(0.66, splitter.positionInRange(splitter.tokenForValue(BigInteger.valueOf(2)), range), 0.01);
        assertEquals(1.0, splitter.positionInRange(end, range), 0.01);
        // Token not in range should return -1.0 for position
        Token notInRange = splitter.tokenForValue(BigInteger.valueOf(10));
        assertEquals(-1.0, splitter.positionInRange(notInRange, range), 0.0);


        // Test medium range
        start = splitter.tokenForValue(BigInteger.ZERO);
        end = splitter.tokenForValue(BigInteger.valueOf(1000));
        range = new Range<>(start, end);
        testPositionInRange(partitioner, splitter, range);

        // Test wrap-around range
        start = splitter.tokenForValue(splitter.valueForToken(partitioner.getMaximumToken()).subtract(BigInteger.valueOf(123456789)));
        end = splitter.tokenForValue(splitter.valueForToken(partitioner.getMinimumToken()).add(BigInteger.valueOf(123456789)));
        range = new Range<>(start, end);
        testPositionInRange(partitioner, splitter, range);

        // Test full range
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMaximumToken(), partitioner.getMaximumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(splitter.tokenForValue(BigInteger.ONE), splitter.tokenForValue(BigInteger.ONE)));
    }

    private void testPositionInRange(IPartitioner partitioner, Splitter splitter, Range<Token> range)
    {
        Range<Token> actualRange = range;
        //full range case
        if (range.left.equals(range.right))
        {
            actualRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        }
        assertEquals(0.0, splitter.positionInRange(actualRange.left, range), 0.01);
        assertEquals(0.25, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.25), range), 0.01);
        assertEquals(0.37, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.373), range), 0.01);
        assertEquals(0.5, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.5), range), 0.01);
        assertEquals(0.75, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.75), range), 0.01);
        assertEquals(0.99, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.999), range), 0.01);
        assertEquals(1.0, splitter.positionInRange(actualRange.right, range), 0.01);
    }

    private Token getTokenInPosition(IPartitioner partitioner, Range<Token> range, double position)
    {
        if (range.left.equals(range.right))
        {
            range = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        }
        Splitter splitter = partitioner.splitter().get();
        BigInteger totalTokens = splitter.tokensInRange(range);
        BigInteger elapsedTokens = BigDecimal.valueOf(position).multiply(new BigDecimal(totalTokens)).toBigInteger();
        BigInteger tokenInPosition = splitter.valueForToken(range.left).add(elapsedTokens);
        return getWrappedToken(partitioner, tokenInPosition);
    }

    private Token getWrappedToken(IPartitioner partitioner, BigInteger position)
    {
        Splitter splitter = partitioner.splitter().get();
        BigInteger maxTokenValue = splitter.valueForToken(partitioner.getMaximumToken());
        BigInteger minTokenValue = splitter.valueForToken(partitioner.getMinimumToken());
        if (position.compareTo(maxTokenValue) > 0)
        {
            position = minTokenValue.add(position.subtract(maxTokenValue));
        }
        return splitter.tokenForValue(position);
    }
}
