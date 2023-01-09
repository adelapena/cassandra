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

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class CompactStoragePagingTest extends UpgradeTestBase
{
    @Test
    public void testPagingWithCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgradesFrom(v30)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE");
            for (int i = 1; i < 10; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)", ConsistencyLevel.ALL, 1, i, i);
        })
        .runAfterNodeUpgrade((cluster, i) -> {
            for (int coord = 1; coord <= 2; coord++)
            {
                Iterator<Object[]> iter = cluster.coordinator(coord).executeWithPaging("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL, 2);
                for (int j = 1; j < 10; j++)
                {
                    Assert.assertTrue(iter.hasNext());
                    Assert.assertArrayEquals(new Object[]{ 1, j, j }, iter.next());
                }
                Assert.assertFalse(iter.hasNext());
            }
        }).run();
    }

    @Test
    public void testPagingWithCompactStorageAndProtocolVersion() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .upgradesFrom(v3X)
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup(c -> {
            c.schemaChange(withKeyspace("CREATE TABLE %s.t (pk text, ck text, v text, PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE"));
            String insert = withKeyspace("INSERT INTO %s.t (pk, ck, v) VALUES (?, ?, ?)");
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, "0", "01", "v");
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, "0", "02", "v");
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            String query = withKeyspace("SELECT * FROM %s.t");
            assertEquals(2, readWithProtocolVersion(query, ProtocolVersion.V5).size());
            assertEquals(2, readWithProtocolVersion(query, ProtocolVersion.V4).size());
            assertEquals(2, readWithProtocolVersion(query, ProtocolVersion.V3).size());
        })
        .run();
    }

    private static List<Row> readWithProtocolVersion(String query, ProtocolVersion protocolVersion)
    {
        Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                  .addContactPoint("127.0.0.1")
                                                                  .withProtocolVersion(protocolVersion);
        try (com.datastax.driver.core.Cluster c = builder.build();
             Session session = c.connect())
        {
            Statement stmt = new SimpleStatement(query);
            stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
            stmt.setFetchSize(1);
            return session.execute(stmt).all();
        }
    }
}
