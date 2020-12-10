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

package org.apache.cassandra.distributed.upgrade;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.shared.Versions;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.READ_REQ;

public class MixedModeAvailabilityTest extends UpgradeTestBase
{
    @Test
    public void testAvailability() throws Throwable
    {
        testAvailability(new AvailabiltyTester(ONE, ALL),
                         new AvailabiltyTester(QUORUM, QUORUM),
                         new AvailabiltyTester(ALL, ONE));
    }

    public void testAvailability(AvailabiltyTester... testers) throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .withConfig(config -> config.set("read_request_timeout_in_ms", SECONDS.toMillis(30))
                                    .set("write_request_timeout_in_ms", SECONDS.toMillis(30)))
        .setup(c -> c.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k uuid, c int, v int, PRIMARY KEY (k, c))")))
        .runAfterNodeUpgrade((cluster, n) -> {

            // using an upgraded and a not upgraded coordinator...
            for (int node = 1; node < cluster.size(); node++)
            {
                ICoordinator coordinator = cluster.coordinator(node);
                int nodeDown = node;

                // using 0 to 2 down nodes...
                for (int numNodesDown = 0; numNodesDown < cluster.size(); numNodesDown++)
                {
                    // disable communications to the down nodes
                    if (numNodesDown > 0)
                    {
                        nodeDown = nextNode(nodeDown, cluster.size());
                        cluster.filters().verbs(READ_REQ.id, MUTATION_REQ.id).to(nodeDown).drop();
                    }

                    // run the test cases that are compatible with the number of down nodes
                    for (AvailabiltyTester tester : testers)
                    {
                        tester.test(coordinator, numNodesDown);
                    }
                }

                // restore communication to all nodes
                cluster.filters().reset();
            }
        }).run();
    }

    private static int nextNode(int node, int numNodes)
    {
        return node == numNodes ? 1 : node + 1;
    }

    private static class AvailabiltyTester
    {
        private static final String INSERT = withKeyspace("INSERT INTO %s.tbl (k, c, v) VALUES (?, ?, ?)");
        private static final String SELECT = withKeyspace("SELECT * FROM %s.tbl WHERE k = ?");

        private final ConsistencyLevel writeConsistencyLevel;
        private final ConsistencyLevel readConsistencyLevel;

        private AvailabiltyTester(ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
        {
            this.writeConsistencyLevel = writeConsistencyLevel;
            this.readConsistencyLevel = readConsistencyLevel;
        }

        public void test(ICoordinator coordinator, int numNodesDown)
        {
            if (numNodesDown > maxNodesDown(writeConsistencyLevel) ||
                numNodesDown > maxNodesDown(readConsistencyLevel))
                return;

            UUID partitionKey = UUID.randomUUID();
            coordinator.execute(INSERT, writeConsistencyLevel, partitionKey, 1, 10);
            coordinator.execute(INSERT, writeConsistencyLevel, partitionKey, 2, 20);
            assertRows(coordinator.execute(SELECT, readConsistencyLevel, partitionKey),
                       row(partitionKey, 1, 10),
                       row(partitionKey, 2, 20));
        }

        private static int maxNodesDown(ConsistencyLevel cl)
        {
            if (cl == ONE)
                return 2;

            if (cl == QUORUM)
                return 1;

            if (cl == ALL)
                return 0;

            throw new IllegalArgumentException("Usupported consistency level: " + cl);
        }
    }
}
