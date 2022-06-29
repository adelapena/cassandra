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
package org.apache.cassandra.cql3;

import java.util.Iterator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;


public class PagingTest
{
    private static Cluster cluster;
    private static Session session;

    private static final String KEYSPACE = "paging_test";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };";

    private static final String dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE;

    @BeforeClass
    public static void setup() throws Exception
    {
        System.setProperty("cassandra.config", "cassandra-murmur.yaml");
        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        // Currently the native server start method return before the server is fully binded to the socket, so we need
        // to wait slightly before trying to connect to it. We should fix this but in the meantime using a sleep.
        Thread.sleep(500);

        cluster = Cluster.builder().addContactPoint("127.0.0.1")
                                   .withPort(DatabaseDescriptor.getNativeTransportPort())
                                   .build();
        session = cluster.connect();

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
    }

    @AfterClass
    public static void tearDown()
    {
        cluster.close();
    }

    /**
     * Makes sure that we don't drop any live rows when paging with DISTINCT queries
     *
     * * We need to have more rows than fetch_size
     * * The node must have a token within the first page (so that the range gets split up in StorageProxy#getRestrictedRanges)
     *   - This means that the second read in the second range will read back too many rows
     * * The extra rows are dropped (so that we only return fetch_size rows to client)
     * * This means that the last row recorded in AbstractQueryPager#recordLast is a non-live one
     * * For the next page, the first row returned will be the same non-live row as above
     * * The bug in CASSANDRA-14956 caused us to drop that non-live row + the first live row in the next page
     */
    @Test
    public void testPaging() throws InterruptedException
    {
        // unrelated change 1
        // unrelated change 2
        throw new IllegalArgumentException("Synthetic error");
    }
}
