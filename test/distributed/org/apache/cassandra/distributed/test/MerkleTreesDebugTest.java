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

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MerkleTreesDebugTest extends TestBaseImpl
{
    private static final int NODES = 6;
    private static final int RF = 3;
    private static final int TABLES = 10;
    private static final int PARTITIONS = 100;
    private static final int CLUSTERINGS = 2;

    @Test
    public void testDefault() throws Throwable
    {
        test("repair", KEYSPACE);
    }

    @Test
    public void testJobs() throws Throwable
    {
        test("repair", "-j", "2", KEYSPACE);
    }

    @Test
    public void testSeq() throws Throwable
    {
        test("repair", "-seq", KEYSPACE);
    }

    private void test(String... nodetoolArgs) throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(NODES)
                                             .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                             .start(), RF))
        {
            cluster.forEach(x -> x.nodetool("disableautocompaction"));
            for (int t = 1; t <= TABLES; t++)
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.t" + t + " (k int, c int, v int, PRIMARY KEY (k, c))"));

            int v = 0;
            for (int k = 0; k < PARTITIONS; k++)
            {
                for (int c = 0; c < CLUSTERINGS; c++)
                {
                    v++;
                    for (int t = 1; t <= TABLES; t++)
                    {
                        String insert = withKeyspace("INSERT INTO %s.t" + t + " (k, c, v) VALUES (?, ?, ?)");
                        cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, k, c, v);
                    }
                }

                if (k % 10 == 0)
                    cluster.forEach(x -> x.flush(KEYSPACE));
            }
            cluster.forEach(x -> x.flush(KEYSPACE));

            NodeToolResult res = cluster.get(1).nodetoolResult(nodetoolArgs);
            res.asserts().success();
        }
    }
}