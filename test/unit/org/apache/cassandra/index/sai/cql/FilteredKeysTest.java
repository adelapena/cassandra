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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

public class FilteredKeysTest extends SAITester
{
    private static final int[] FETCH_SIZES = new int[]{ 1, 2, 3, 4, Integer.MAX_VALUE };

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        requireNetwork();
    }

    @Test
    public void testFilteredFirstClustering()
    {
        createTable("CREATE TABLE %s (k int, c1 text, c2 int, PRIMARY KEY (k, c1, c2))");
        createIndex("CREATE INDEX ON %s(c1) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        execute("INSERT INTO %s (k, c1, c2) VALUES (1, 'A', 1)");
        execute("INSERT INTO %s (k, c1, c2) VALUES (1, 'A', 2)");
        execute("INSERT INTO %s (k, c1, c2) VALUES (2, 'A', 3)");
        execute("INSERT INTO %s (k, c1, c2) VALUES (2, 'A', 4)");

        assertRows("SELECT c2 FROM %s WHERE c1 = 'a'", row(1), row(2), row(3), row(4));
        assertRows("SELECT c2 FROM %s WHERE c1 = 'a' AND c2 = 4 ALLOW FILTERING", row(4));
        assertRows("SELECT c2 FROM %s WHERE c1 = 'a' AND c2 > 2 ALLOW FILTERING", row(3), row(4));

        assertRows("SELECT c2 FROM %s WHERE k = 2 AND c1 = 'a'", row(3), row(4));
        assertRows("SELECT c2 FROM %s WHERE k = 2 AND c1 = 'a' AND c2 = 4 ALLOW FILTERING", row(4));
        assertRows("SELECT c2 FROM %s WHERE k = 2 AND c1 = 'a' AND c2 > 2 ALLOW FILTERING", row(3), row(4));
    }

    @Test
    public void testFilteredLastClustering()
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, PRIMARY KEY (k, c1, c2))");
        createIndex("CREATE INDEX ON %s(c2) USING 'sai' WITH OPTIONS = { 'case_sensitive' : false }");

        execute("INSERT INTO %s (k, c1, c2) VALUES (1, 1, 'A')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (1, 2, 'A')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (2, 3, 'A')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (2, 4, 'A')");

        assertRows("SELECT c1 FROM %s WHERE c2 = 'a'", row(1), row(2), row(3), row(4));
        assertRows("SELECT c1 FROM %s WHERE c1 = 1 AND c2 = 'a' ALLOW FILTERING", row(1));
        assertRows("SELECT c1 FROM %s WHERE c1 > 2 AND c2 = 'a' ALLOW FILTERING", row(3), row(4));

        assertRows("SELECT c1 FROM %s WHERE k = 2 AND c2 = 'a'", row(3), row(4));
        assertRows("SELECT c1 FROM %s WHERE k = 2 AND c1 = 3 AND c2 = 'a'", row(3));
        assertRows("SELECT c1 FROM %s WHERE k = 2 AND c1 > 2 AND c2 = 'a'", row(3), row(4));
    }

    private void assertRows(String query, Object[]... expectedRows)
    {
        assertRows(execute(query), expectedRows);
        for (int fetchSize : FETCH_SIZES)
        {
            assertRowsNet(executeNetWithPaging(query, fetchSize), expectedRows);
        }
    }
}
