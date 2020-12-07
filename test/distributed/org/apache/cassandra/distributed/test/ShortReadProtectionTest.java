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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

import static com.google.common.collect.Iterators.toArray;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests short read protection, the mechanism that ensures distributed queries at read consistency levels > ONE/LOCAL_ONE
 * avoid short reads that might happen when a limit is used and reconciliation accepts less rows than such limit.
 */
@RunWith(Parameterized.class)
public class ShortReadProtectionTest extends TestBaseImpl
{
    private static final int NUM_NODES = 3;
    private static final int[] PAGE_SIZES = new int[]{ 1, 2, 3, 4, 1000 };

    private static Cluster cluster;
    private Tester tester;

    /**
     * {@code true} for CL=QUORUM writes and CL=QUORUM reads, {@code false} for CL=ONE writes and CL=ALL reads.
     */
    @Parameterized.Parameter
    public boolean quorum;

    /**
     * Whether to flush data after mutations.
     */
    @Parameterized.Parameter(1)
    public boolean flush;

    /**
     * Whether paging is used for the distributed queries.
     */
    @Parameterized.Parameter(2)
    public boolean paging;

    /**
     * The node to be used as coordinator.
     */
    @Parameterized.Parameter(3)
    public int coordinator;

    @Parameterized.Parameters(name = "{index}: quorum={0} flush={1} paging={2} coordinator={3}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (boolean quorum : BOOLEANS)
            for (boolean flush : BOOLEANS)
                for (boolean paging : BOOLEANS)
                    for (int coordinator = 1; coordinator < NUM_NODES; coordinator++)
                        result.add(new Object[]{ quorum, flush, paging, coordinator });
        return result;
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = init(Cluster.build()
                              .withNodes(NUM_NODES)
                              .withConfig(config -> config.set("hinted_handoff_enabled", false))
                              .start());
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void setupTester()
    {
        tester = new Tester(quorum, flush, paging, coordinator);
    }

    @After
    public void teardownTester()
    {
        tester.dropTable();
    }

    /**
     * Tests SRP for tables with no clustering columns and with a deleted row.
     * <p>
     * See CASSANDRA-13880.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13880()}.
     */
    @Test
    public void testSkinnyTableWithoutLiveRows()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes("INSERT INTO %s (id) VALUES (0) USING TIMESTAMP 0")
              .onlyNode1("DELETE FROM %s WHERE id = 0")
              .assertRows("SELECT DISTINCT id FROM %s WHERE id = 0")
              .assertRows("SELECT id FROM %s WHERE id = 0 LIMIT 1");
    }

    /**
     * Tests SRP for tables with no clustering columns and with alternated live and deleted rows.
     * <p>
     * See CASSANDRA-13747.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13747()}.
     */
    @Test
    public void testSkinnyTableWithLiveRows()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes(0, 10, i -> format("INSERT INTO %%s (id) VALUES (%d) USING TIMESTAMP 0", i)) // order is 5,1,8,0,2,4,7,6,9,3
              .onlyNode1("DELETE FROM %s WHERE id IN (1, 0, 4, 6, 3)") // delete every other row
              .assertRows("SELECT DISTINCT token(id), id FROM %s",
                          row(token(5), 5), row(token(8), 8), row(token(2), 2), row(token(7), 7), row(token(9), 9));
    }

    /**
     * Tests SRP for tables with no clustering columns and with complementary deleted rows.
     * <p>
     * See CASSANDRA-13595.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13595()}.
     */
    @Test
    public void testSkinnyTableWithComplementaryDeletions()
    {
        tester.createTable("CREATE TABLE %s (id int PRIMARY KEY)")
              .allNodes(0, 10, i -> format("INSERT INTO %%s (id) VALUES (%d) USING TIMESTAMP 0", i)) // order is 5,1,8,0,2,4,7,6,9,3
              .onlyNode1("DELETE FROM %s WHERE id IN (5, 8, 2, 7, 9)") // delete every other row
              .onlyNode2("DELETE FROM %s WHERE id IN (1, 0, 4, 6)") // delete every other row but the last one
              .assertRows("SELECT id FROM %s LIMIT 1", row(3))
              .assertRows("SELECT DISTINCT id FROM %s LIMIT 1", row(3));
    }

    /**
     * Tests SRP when more than one row is missing.
     * <p>
     * See CASSANDRA-12872.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_12872()}.
     */
    @Test
    public void testMultipleMissedRows()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .allNodes(0, 4, i -> format("INSERT INTO %%s (pk, ck) VALUES (0, %d) USING TIMESTAMP 0", i))
              .onlyNode1("DELETE FROM %s WHERE pk = 0 AND ck IN (1, 2, 3)",
                         "INSERT INTO %s (pk, ck) VALUES (0, 5)")
              .onlyNode2("INSERT INTO %s (pk, ck) VALUES (0, 4)")
              .assertRows("SELECT ck FROM %s WHERE pk = 0 LIMIT 2", row(0), row(4));
    }

    /**
     * Tests SRP with deleted rows at the beginning of the partition and ascending order.
     * <p>
     * See CASSANDRA-9460.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read()} together with
     * {@link #testDescendingOrder()}.
     */
    @Test
    public void testAscendingOrder()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes(1, 10, i -> format("INSERT INTO %%s (k, c, v) VALUES (0, %d, %d) USING TIMESTAMP 0", i, i * 10))
              .onlyNode1("DELETE FROM %s WHERE k=0 AND c=1")
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .onlyNode3("DELETE FROM %s WHERE k=0 AND c=3")
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 1", row(4, 40))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 2", row(4, 40), row(5, 50))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 3", row(4, 40), row(5, 50), row(6, 60))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c ASC LIMIT 4", row(4, 40), row(5, 50), row(6, 60), row(7, 70));
    }

    /**
     * Tests SRP behaviour with deleted rows at the end of the partition and descending order.
     * <p>
     * See CASSANDRA-9460.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read()} together with
     * {@link #testAscendingOrder()}.
     */
    @Test
    public void testDescendingOrder()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes(1, 10, i -> format("INSERT INTO %%s (k, c, v) VALUES (0, %d, %d) USING TIMESTAMP 0", i, i * 10))
              .onlyNode1("DELETE FROM %s WHERE k=0 AND c=7")
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=8")
              .onlyNode3("DELETE FROM %s WHERE k=0 AND c=9")
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 1", row(6, 60))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 2", row(6, 60), row(5, 50))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 3", row(6, 60), row(5, 50), row(4, 40))
              .assertRows("SELECT c, v FROM %s WHERE k=0 ORDER BY c DESC LIMIT 4", row(6, 60), row(5, 50), row(4, 40), row(3, 30));
    }

    /**
     * Test short reads ultimately leaving no rows alive after a partition deletion.
     * <p>
     * See CASSANDRA-4000 and CASSANDRA-8933.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_short_read_delete()} and
     * {@code consistency_test.py:TestConsistency.test_short_read_quorum_delete()}. Note that the {@link #quorum} test
     * parameter ensures that both tests are covered.
     */
    @Test
    public void testDeletePartition()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v) VALUES (0, 1, 10) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .onlyNode2("DELETE FROM %s WHERE k=0")
              .assertRows("SELECT c, v FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a partition deletion when there is a static row.
     */
    @Test
    public void testDeletePartitionWithStatic()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, s int STATIC, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v, s) VALUES (0, 1, 10, 100) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .onlyNode2("DELETE FROM %s WHERE k=0")
              .assertRows("SELECT c, v FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a clustering deletion.
     */
    @Test
    public void testDeleteClustering()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v) VALUES (0, 1, 10) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=1")
              .assertRows("SELECT * FROM %s WHERE k=0 LIMIT 1", row(0, 2, 20))
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .assertRows("SELECT * FROM %s WHERE k=0 LIMIT 1");
    }

    /**
     * Test short reads ultimately leaving no rows alive after a clustering deletion when there is a static row.
     */
    @Test
    public void testDeleteClusteringWithStatic()
    {
        tester.createTable("CREATE TABLE %s (k int, c int, v int, s int STATIC, PRIMARY KEY(k, c))")
              .allNodes("INSERT INTO %s (k, c, v, s) VALUES (0, 1, 10, 100) USING TIMESTAMP 0",
                        "INSERT INTO %s (k, c, v) VALUES (0, 2, 20) USING TIMESTAMP 0")
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=1")
              .assertRows("SELECT k, c, v, s FROM %s WHERE k=0 LIMIT 1", row(0, 2, 20, 100))
              .onlyNode2("DELETE FROM %s WHERE k=0 AND c=2")
              .assertRows("SELECT k, c, v, s FROM %s WHERE k=0 LIMIT 1", row(0, null, null, 100));
    }

    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and regular row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupByRegularRow()
    {
        Assume.assumeFalse(paging); // paging fails due to CASSANDRA-16307
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .onlyNode1("INSERT INTO %s (pk, ck) VALUES (1, 1) USING TIMESTAMP 0",
                         "DELETE FROM %s WHERE pk=0 AND ck=0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 2) USING TIMESTAMP 0")
              .onlyNode2("DELETE FROM %s WHERE pk=1 AND ck=1",
                         "INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                         "DELETE FROM %s WHERE pk=2 AND ck=2")
              .assertRows("SELECT * FROM %s LIMIT 1")
              .assertRows("SELECT * FROM %s LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 10");
    }

    /**
     * Test GROUP BY with short read protection, particularly when there is a limit and static row deletions.
     * <p>
     * See CASSANDRA-15459
     */
    @Test
    public void testGroupByStaticRow()
    {
        Assume.assumeFalse(paging); // paging fails due to CASSANDRA-16307
        tester.createTable("CREATE TABLE %s (pk int, ck int, s int static, PRIMARY KEY (pk, ck))")
              .onlyNode1("INSERT INTO %s (pk, s) VALUES (1, 1) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, s) VALUES (0, null)",
                         "INSERT INTO %s (pk, s) VALUES (2, 2) USING TIMESTAMP 0")
              .onlyNode2("INSERT INTO %s (pk, s) VALUES (1, null)",
                         "INSERT INTO %s (pk, s) VALUES (0, 0) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, s) VALUES (2, null)")
              .assertRows("SELECT * FROM %s LIMIT 1")
              .assertRows("SELECT * FROM %s LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk LIMIT 10")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 1")
              .assertRows("SELECT * FROM %s GROUP BY pk, ck LIMIT 10");
    }

    /**
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911()}.
     */
    @Test
    public void test13911()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .onlyNode1("INSERT INTO %s (pk, ck) VALUES (0, 0)")
              .onlyNode2("DELETE FROM %s WHERE pk = 0 AND ck IN (1, 2)")
              .assertRows("SELECT DISTINCT pk FROM %s", row(0));
    }

    /**
     * A regression test to prove that we can no longer rely on {@code !singleResultCounter.isDoneForPartition()} to
     * abort single partition SRP early if a per partition limit is set.
     * <p>
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911_rows_srp()}.
     */
    @Test
    public void test13911rows()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .onlyNode1("INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (0, 1) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 0) USING TIMESTAMP 0",
                         "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck = 1")
              .onlyNode2("INSERT INTO %s (pk, ck) VALUES (0, 2) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (0, 3) USING TIMESTAMP 0",
                         "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck = 0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 1) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 2) USING TIMESTAMP 0")
              .assertRows("SELECT pk, ck FROM %s PER PARTITION LIMIT 2 LIMIT 3", row(0, 0), row(0, 1), row(2, 2));
    }

    /**
     * A regression test to prove that we can no longer rely on {@code !singleResultCounter.isDone()} to abort ranged
     * partition SRP early if a per partition limit is set.
     * <p>
     * See CASSANDRA-13911.
     * <p>
     * Replaces Python dtest {@code consistency_test.py:TestConsistency.test_13911_partitions_srp()}.
     */
    @Test
    public void test13911partitions()
    {
        tester.createTable("CREATE TABLE %s (pk int, ck int, PRIMARY KEY (pk, ck))")
              .onlyNode1("INSERT INTO %s (pk, ck) VALUES (0, 0) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (0, 1) USING TIMESTAMP 0",
                         "DELETE FROM %s USING TIMESTAMP 42 WHERE pk = 2 AND ck IN  (0, 1)")
              .onlyNode2("INSERT INTO %s (pk, ck) VALUES (0, 2) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (0, 3) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 0) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (2, 1) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (4, 0) USING TIMESTAMP 0",
                         "INSERT INTO %s (pk, ck) VALUES (4, 1) USING TIMESTAMP 0")
              .assertRows("SELECT pk, ck FROM %s PER PARTITION LIMIT 2 LIMIT 4",
                          row(0, 0), row(0, 1), row(4, 0), row(4, 1));
    }

    private static long token(int key)
    {
        return (long) Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(key)).getTokenValue();
    }

    private static class Tester
    {
        private static final AtomicInteger seqNumber = new AtomicInteger();

        private final ICoordinator coordinator;
        private final boolean quorum, flush, paging;
        private final String qualifiedTableName;
        private final ConsistencyLevel consistencyLevel;

        private boolean flushed = false;

        private Tester(boolean quorum, boolean flush, boolean paging, int coordinator)
        {
            this.coordinator = cluster.coordinator(coordinator);
            this.quorum = quorum;
            this.flush = flush;
            this.paging = paging;
            qualifiedTableName = KEYSPACE + ".t_" + seqNumber.getAndIncrement();
            consistencyLevel = quorum ? QUORUM : ALL;
        }

        private Tester createTable(String query)
        {
            cluster.schemaChange(format(query) + " WITH read_repair='NONE'");
            return this;
        }

        private Tester allNodes(int startInclusive, int endExclusive, Function<Integer, String> querySupplier)
        {
            IntStream.range(startInclusive, endExclusive).mapToObj(querySupplier::apply).forEach(this::allNodes);
            return this;
        }

        private Tester allNodes(String... queries)
        {
            for (String query : queries)
                allNodes(query);
            return this;
        }

        private Tester allNodes(String query)
        {
            coordinator.execute(format(query), ALL);
            return this;
        }

        private Tester onlyNode1(String... queries)
        {
            onlyNodeN(1, queries);
            return this;
        }

        private Tester onlyNode2(String... queries)
        {
            onlyNodeN(2, queries);
            return this;
        }

        private Tester onlyNode3(String... queries)
        {
            onlyNodeN(3, queries);
            return this;
        }

        private void onlyNodeN(int node, String... queries)
        {
            for (String query : queries)
            {
                String formattedQuery = format(query);
                cluster.get(node).executeInternal(formattedQuery);
                if (quorum)
                    cluster.get(node == NUM_NODES ? 1 : node + 1).executeInternal(formattedQuery);
            }
        }

        private Tester assertRows(String query, Object[]... expectedRows)
        {
            if (flush && !flushed)
            {
                cluster.stream().forEach(n -> n.flush(KEYSPACE));
                flushed = true;
            }

            query = format(query);
            for (int fetchSize : PAGE_SIZES)
            {
                Object[][] actualRows = paging
                                        ? toArray(coordinator.executeWithPaging(query, consistencyLevel, fetchSize),
                                                  Object[].class)
                                        : coordinator.execute(query, consistencyLevel);
                AssertUtils.assertRows(actualRows, expectedRows);
            }

            return this;
        }

        private String format(String query)
        {
            return String.format(query, qualifiedTableName);
        }

        private void dropTable()
        {
            cluster.schemaChange(format("DROP TABLE IF EXISTS %s"));
        }
    }
}