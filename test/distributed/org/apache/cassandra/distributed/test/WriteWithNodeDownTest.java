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
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;

public class WriteWithNodeDownTest extends TestBaseImpl
{
    private static final int NUM_ROWS = 100;

    @Test
    public void testHintsServiceMetrics() throws Exception
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withConfig(c -> c.set("hinted_handoff_enabled", false)
                                                          .with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v int)"));

            cluster.get(2).shutdown().get();

            for (int i = 0; i < NUM_ROWS; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (k, v) VALUES (?, ?)"), ONE, i, i);

            cluster.get(2).startup();

            assertThat(countRows(cluster.get(1))).isEqualTo(NUM_ROWS);
            assertThat(countRows(cluster.get(2))).isZero();
        }
    }

    private static int countRows(IInvokableInstance node)
    {
        return node.executeInternal(withKeyspace("SELECT * FROM %s.t")).length;
    }
}
