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

package org.apache.cassandra.cql3.functions.masking;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;

import static java.lang.String.format;

@RunWith(Enclosed.class)
public class QueryMaskedColumnsTest extends CQLTester
{
    @RunWith(Parameterized.class)
    public static class ParameterizedTests extends QueryMaskedColumnsTest
    {
        @Parameterized.Parameter
        public String mask;

        @Parameterized.Parameter(1)
        public String columnType;

        @Parameterized.Parameter(2)
        public Object columnValue;

        @Parameterized.Parameter(3)
        public Object maskedValue;

        @Parameterized.Parameters(name = "mask={0}, type={1}, value={2}")
        public static Collection<Object[]> options()
        {
            return Arrays.asList(new Object[][]{
            { "DEFAULT", "text", "abc", "****" },
            { "DEFAULT", "int", 123, 0 },
            { "mask_default()", "text", "abc", "****" },
            { "mask_default()", "int", 123, 0, },

            { "mask_null()", "text", "abc", null },
            { "mask_null()", "int", 123, null },

            { "mask_replace('redacted')", "ascii", "abc", "redacted" },
            { "mask_replace((text) 'redacted')", "text", "abc", "redacted" },
            { "mask_replace(0)", "int", 123, 0 },
            { "mask_replace((bigint) 0)", "bigint", 123L, 0L },
            { "mask_replace((varint) 0)", "varint", BigInteger.valueOf(123), BigInteger.ZERO },

            { "mask_inner(1, 2)", "text", "abcdef", "a***ef" },
            { "mask_inner(1, 2, '#')", "text", "abcdef", "a###ef" },
            { "mask_outer(1, 2)", "text", "abcdef", "*bcd**" },
            { "mask_outer(1, 2, '#')", "text", "abcdef", "#bcd##", },
            });
        }

        @BeforeClass
        public static void beforeClass() throws Throwable
        {
            requireNetwork();
        }

        @Before
        public void before() throws Throwable
        {
            createTable("CREATE TABLE %s (" +
                        format("k1 %s, k2 %<s MASKED WITH %s, ", columnType, mask) +
                        format("c1 %s, c2 %<s MASKED WITH %s, ", columnType, mask) +
                        format("r1 %s, r2 %<s MASKED WITH %s, ", columnType, mask) +
                        format("s1 %s static, s2 %<s static MASKED WITH %s, ", columnType, mask) +
                        "PRIMARY KEY((k1, k2), c1, c2))");

            createView("CREATE MATERIALIZED VIEW %s AS SELECT k2, k1, c2, c1, r2, r1 FROM %s " +
                       "WHERE k1 IS NOT NULL AND k2 IS NOT NULL " +
                       "AND c1 IS NOT NULL AND c2 IS NOT NULL " +
                       "AND r1 IS NOT NULL AND r2 IS NOT NULL " +
                       "PRIMARY KEY ((c2, c1), k2, k1)");

            execute("INSERT INTO %s(k1, k2, c1, c2, r1, r2, s1, s2) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    columnValue, columnValue, columnValue, columnValue, columnValue, columnValue, columnValue, columnValue);
        }

        @Test
        public void testSelectWithWilcard() throws Throwable
        {
            UntypedResultSet rs = execute("SELECT * FROM %s");
            assertColumnNames(rs,
                              "k1", maskedColumnName("k2"),
                              "c1", maskedColumnName("c2"),
                              "s1", maskedColumnName("s2"),
                              "r1", maskedColumnName("r2"));
            assertRows(rs, row(columnValue, maskedValue,
                               columnValue, maskedValue,
                               columnValue, maskedValue,
                               columnValue, maskedValue));

            rs = execute(format("SELECT * FROM %s.%s", KEYSPACE, currentView()));
            assertColumnNames(rs,
                              maskedColumnName("c2"), "c1",
                              maskedColumnName("k2"), "k1",
                              "r1", maskedColumnName("r2"));
            assertRows(rs, row(maskedValue, columnValue,
                               maskedValue, columnValue,
                               columnValue, maskedValue));
        }

        @Test
        public void testSelectWithAllColumnNames() throws Throwable
        {
            UntypedResultSet rs = execute("SELECT c2, c1, k2, k1, r2, r1, s2, s1 FROM %s");
            assertColumnNames(rs,
                              maskedColumnName("c2"), "c1",
                              maskedColumnName("k2"), "k1",
                              maskedColumnName("r2"), "r1",
                              maskedColumnName("s2"), "s1");
            assertRows(rs, row(maskedValue, columnValue,
                               maskedValue, columnValue,
                               maskedValue, columnValue,
                               maskedValue, columnValue));

            rs = execute(format("SELECT c2, c1, k2, k1, r2, r1 FROM %s.%s", KEYSPACE, currentView()));
            assertColumnNames(rs,
                              maskedColumnName("c2"), "c1",
                              maskedColumnName("k2"), "k1",
                              maskedColumnName("r2"), "r1");
            assertRows(rs, row(maskedValue, columnValue,
                               maskedValue, columnValue,
                               maskedValue, columnValue));
        }

        @Test
        public void testSelectOnlyMaskedColumns() throws Throwable
        {
            UntypedResultSet rs = execute("SELECT k2, c2, s2, r2 FROM %s");
            assertColumnNames(rs,
                              maskedColumnName("k2"),
                              maskedColumnName("c2"),
                              maskedColumnName("s2"),
                              maskedColumnName("r2"));
            assertRows(rs, row(maskedValue, maskedValue, maskedValue, maskedValue));

            rs = execute(format("SELECT k2, c2, r2 FROM %s.%s", KEYSPACE, currentView()));
            assertColumnNames(rs, maskedColumnName("k2"), maskedColumnName("c2"), maskedColumnName("r2"));
            assertRows(rs, row(maskedValue, maskedValue, maskedValue));
        }

        @Test
        public void testSelectOnlyNotMaskedColumns() throws Throwable
        {
            UntypedResultSet rs = execute("SELECT k1, c1, s1, r1 FROM %s");
            assertColumnNames(rs, "k1", "c1", "s1", "r1");
            assertRows(rs, row(columnValue, columnValue, columnValue, columnValue));

            rs = execute(format("SELECT k1, c1, r1 FROM %s.%s", KEYSPACE, currentView()));
            assertColumnNames(rs, "k1", "c1", "r1");
            assertRows(rs, row(columnValue, columnValue, columnValue));
        }

        private String maskedColumnName(String columnName)
        {
            return columnName;
        }
    }
}
