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

import java.util.function.Consumer;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.RowUtil;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.awaitility.Awaitility.await;

/**
 * Tests for dynamic data masking.
 */
public class ColumnMaskTest extends TestBaseImpl
{
    /**
     * Tests that column masks using UDFs are correctly loaded on startup.
     * The UDF should be loaded before it's referenced by the mask.
     */
    @Test
    public void testUDFMaskedColumnsOnStartup() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(1)
                                           .withConfig(conf -> conf.with(GOSSIP, NATIVE_PROTOCOL)
                                                                   .set("user_defined_functions_enabled", "true")
                                                                   .set("authenticator", "PasswordAuthenticator")
                                                                   .set("authorizer", "CassandraAuthorizer"))
                                           .start()))
        {
            IInvokableInstance node = cluster.get(1);

            // create a table with a column masked with a UDF
            cluster.schemaChange(withKeyspace("CREATE FUNCTION %s.f(column text, replacement text) " +
                                              "RETURNS NULL ON NULL INPUT " +
                                              "RETURNS text " +
                                              "LANGUAGE java " +
                                              "AS 'return replacement;'"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, v text MASKED WITH %<s.f('redacted'))"));
            node.executeInternal(withKeyspace("INSERT INTO %s.t(k, v) VALUES (0, 'secret')"));

            // create a user without UNMASK permission
            withAuthenticatedSession(node, DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
                session.execute("CREATE USER test WITH PASSWORD 'test'");
                session.execute(withKeyspace("GRANT ALL ON KEYSPACE %s TO test"));
                session.execute(withKeyspace("REVOKE UNMASK ON KEYSPACE %s FROM test"));
            });

            // restart the node, so the schema elements (the UDF and the mask) have to be loaded
            node.shutdown().get();
            node.startup();

            // verify that the user without UNMASK permission can't see the clear data
            withAuthenticatedSession(node, "test", "test", session -> {
                ResultSet resultSet = session.execute(withKeyspace("SELECT * FROM %s.t"));
                assertRows(RowUtil.toObjects(resultSet), row(0, "redacted"));
            });
        }
    }

    private static void withAuthenticatedSession(IInvokableInstance instance, String username, String password, Consumer<Session> consumer)
    {
        // wait for existing roles
        await().pollDelay(1, SECONDS)
               .pollInterval(1, SECONDS)
               .atMost(1, MINUTES)
               .until(() -> instance.callOnInstance(CassandraRoleManager::hasExistingRoles));

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");
        try (com.datastax.driver.core.Cluster c = builder.withCredentials(username, password).build();
             Session session = c.connect())
        {
            consumer.accept(session);
        }
    }
}
