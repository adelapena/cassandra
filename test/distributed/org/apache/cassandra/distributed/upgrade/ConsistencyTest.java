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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.Versions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.*;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class ConsistencyTest extends UpgradeTestBase
{
    @Test
    public void testConsistency() throws Throwable
    {
        testConsistency(allUpgrades(3, 1));
    }

    @Test
    public void testConsistencyWithNetworkAndGossip() throws Throwable
    {
        testConsistency(new TestCase().nodes(3)
                                      .nodesToUpgrade(1)
                                      // .upgrade(Versions.Major.v22, Versions.Major.v30)
                                      // .upgrade(Versions.Major.v22, Versions.Major.v3X)
                                      // .upgrade(Versions.Major.v30, Versions.Major.v3X)
                                      .upgrade(Versions.Major.v30, Versions.Major.v4)
                                      .upgrade(Versions.Major.v3X, Versions.Major.v4)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK, Feature.GOSSIP)));
    }

    private void testConsistency(TestCase testCase) throws Throwable
    {
        List<ConsistencyTester> testers = new ArrayList<>();
        testers.addAll(ConsistencyTester.create(1, ALL, THREE));
        testers.addAll(ConsistencyTester.create(2, ALL, THREE, QUORUM, LOCAL_QUORUM, TWO));
        testers.addAll(ConsistencyTester.create(3, ALL, THREE, QUORUM, LOCAL_QUORUM, TWO, ONE, LOCAL_ONE));

        testCase.setup(cluster -> {
            ConsistencyTester.createTable(cluster);
            for (ConsistencyTester tester : testers)
                tester.writeRows(cluster);
        }).runAfterNodeAndClusterUpgrade(cluster -> {
            for (ConsistencyTester tester : testers)
                tester.readRows(cluster);
        }).run();
    }

    private static class ConsistencyTester
    {
        private final int numWrittenReplicas;
        private final ConsistencyLevel readConsistencyLevel;
        private final UUID partitionKey;

        private ConsistencyTester(int numWrittenReplicas, ConsistencyLevel readConsistencyLevel)
        {
            this.numWrittenReplicas = numWrittenReplicas;
            this.readConsistencyLevel = readConsistencyLevel;
            partitionKey = UUID.randomUUID();
        }

        private static List<ConsistencyTester> create(int numWrittenReplicas, ConsistencyLevel... readConsistencyLevels)
        {
            return Stream.of(readConsistencyLevels)
                         .map(readConsistencyLevel -> new ConsistencyTester(numWrittenReplicas, readConsistencyLevel))
                         .collect(Collectors.toList());
        }

        private static void createTable(UpgradeableCluster cluster)
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k uuid, c int, v int, PRIMARY KEY (k, c))"));
        }

        private void writeRows(UpgradeableCluster cluster)
        {
            String query = withKeyspace("INSERT INTO %s.tbl (k, c, v) VALUES (?, ?, ?)");
            for (int i = 1; i <= numWrittenReplicas; i++)
            {
                IUpgradeableInstance node = cluster.get(i);
                node.executeInternal(query, partitionKey, 1, 10);
                node.executeInternal(query, partitionKey, 2, 20);
                node.executeInternal(query, partitionKey, 3, 30);
            }
        }

        private void readRows(UpgradeableCluster cluster)
        {
            String query = withKeyspace("SELECT * FROM %s.tbl WHERE k = ?");
            for (ICoordinator coordinator : cluster.coordinators())
            {
                assertRows(coordinator.execute(query, readConsistencyLevel, partitionKey),
                           row(partitionKey, 1, 10),
                           row(partitionKey, 2, 20),
                           row(partitionKey, 3, 30));
            }
        }
    }
}
