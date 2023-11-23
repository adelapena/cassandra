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

package org.apache.cassandra.distributed.test.guardrails;

import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the guardrails for the number of keyspaces and tables, {@link Guardrails#keyspaces} and {@link Guardrails#tables},
 * disable the old config properties they replace, {@code keyspace_count_warn_threshold} and {@code table_count_warn_threshold}.
 */
public class GuardrailDeprecationTest extends TestBaseImpl
{
    private static final String LOG_DEPRECATION_MESSAGE = "parameters have been deprecated";

    @Test
    public void testDefaults() throws Throwable
    {
        assertWarnsAboutDeprecation(false, c -> {});
    }

    @Test
    public void testEnableNewProperties() throws Throwable
    {
        assertWarnsAboutDeprecation(false, c -> c.set("keyspaces_warn_threshold", 1)
                                                .set("tables_warn_threshold", 1));
    }

    @Test
    public void testEnableOldProperties() throws Throwable
    {
        assertWarnsAboutDeprecation(true, c -> c.set("keyspace_count_warn_threshold", 100)
                                                .set("table_count_warn_threshold", 200));
    }

    @Test
    public void testEnableOldAndNewProperties() throws Throwable
    {
        assertWarnsAboutDeprecation(true, c -> c.set("keyspace_count_warn_threshold", 100)
                                                .set("table_count_warn_threshold", 200)
                                                .set("keyspaces_warn_threshold", 300)
                                                .set("tables_warn_threshold", 400));
    }

    private static void assertWarnsAboutDeprecation(boolean shouldWarn, Consumer<IInstanceConfig> config) throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(config).start(), 1))
        {
            assertThat(cluster.get(1).logs().grep(LOG_DEPRECATION_MESSAGE).getResult()).hasSize(shouldWarn ? 1 : 0);
        }
    }
}
