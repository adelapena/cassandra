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

package org.apache.cassandra.index.sai.cql;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.junit.Test;

public class InQueryTest extends CQLTester
{
    @Test
    public void notInTest() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        execute("INSERT INTO %s (k, v1, v2) VALUES (1, 1, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (2, 2, 2)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (3, 3, 1)");
        execute("INSERT INTO %s (k, v1, v2) VALUES (4, 4, 2)");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v1 IN (1, 2)");

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 1");

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 1 ALLOW FILTERING"),
                                row(1, 1, 1));

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1 IN (1, 2) AND v2 = 2 ALLOW FILTERING"),
                                row(2, 2, 2));
    }
}
