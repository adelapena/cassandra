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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class UnsupportedExpressionsTest extends TestBaseImpl
{
    @Test
    public void shouldRejectNonStrictIN() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config -> config.with(GOSSIP).with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, a int, b int)"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(a) USING 'sai'"));
            cluster.schemaChange(withKeyspace("CREATE INDEX ON %s.t(b) USING 'sai'"));

            // insert an unrepaired row
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t(k, a) VALUES (0, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.t(k, b) VALUES (0, 2)"));

            String select = withKeyspace("SELECT * FROM %s.t WHERE a = 1 AND b IN (2, 3) ALLOW FILTERING");
            
            // This should fail, as strict filtering is not allowed:
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(select, ConsistencyLevel.ALL))
                      .hasMessageContaining(String.format(StorageAttachedIndexQueryPlan.UNSUPPORTED_NON_STRICT_OPERATOR, Operator.IN));
            
            // Repair fixes the split row, although we still only allow the query when reconciliation is not required:
            cluster.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
            assertRows(cluster.coordinator(1).execute(select, ConsistencyLevel.ONE), row(0, 1, 2));
        }
    }
}
