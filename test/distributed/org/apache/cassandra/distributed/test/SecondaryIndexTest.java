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

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.SchemaConstants;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.shared.AssertUtils.*;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.awaitility.Awaitility.await;

public class SecondaryIndexTest extends TestBaseImpl
{
    @Test
    public void indexCorrectlyMarkedAsBuildAndRemoved() throws Throwable
    {
        String query = String.format("SELECT * FROM %s.\"%s\"",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BUILT_INDEXES);

        try (Cluster cluster = init(Cluster.create(2)))
        {
            // check that initially there are no rows in the built indexes table
            cluster.forEach(node -> assertRows(node.executeInternal(query)));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (a int, b int, c int, PRIMARY KEY (a, b))"));
            cluster.schemaChange(withKeyspace("CREATE INDEX idx ON %s.t(c)"));

            for (int i = 1; i <= cluster.size(); i++)
            {
                IInvokableInstance node = cluster.get(i);
                waitForIndex(node, KEYSPACE, "idx");

                // check that there are no other rows in the built indexes table
                Object[] row = row(KEYSPACE, "idx", null);
                assertRows(node.executeInternal(query), row);

                // rebuild the index and verify the built status table
                node.nodetool("rebuild_index", KEYSPACE, "t", "idx");
                waitForIndex(node, KEYSPACE, "idx");

                // check that there are no other rows in the built indexes table
                assertRows(node.executeInternal(query), row);
            }

            // check that dropping the index removes it from the built indexes table
            cluster.schemaChange(withKeyspace("DROP INDEX %s.idx"));
            cluster.forEach(node -> assertRows(node.executeInternal(query)));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void waitForIndex(IInvokableInstance node, String keyspace, String index)
    {
        String query = String.format("SELECT * FROM %s.\"%s\" WHERE table_name=? AND index_name=?",
                                     SchemaConstants.SYSTEM_KEYSPACE_NAME,
                                     SystemKeyspace.BUILT_INDEXES);
        await().atMost(30, SECONDS)
               .pollDelay(1, SECONDS)
               .until(() -> node.executeInternal(query, keyspace, index).length == 1);
    }
}
