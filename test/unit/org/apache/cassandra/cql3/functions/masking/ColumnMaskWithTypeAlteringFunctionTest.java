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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQL3Type;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.CQL3Type.Native.ASCII;
import static org.apache.cassandra.cql3.CQL3Type.Native.BIGINT;
import static org.apache.cassandra.cql3.CQL3Type.Native.BLOB;
import static org.apache.cassandra.cql3.CQL3Type.Native.INT;
import static org.apache.cassandra.cql3.CQL3Type.Native.TEXT;
import static org.apache.cassandra.cql3.CQL3Type.Native.VARINT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ColumnMaskTester} for masks using functions that might return values with a type different to the type of the
 * masked column. Those queries should fail.
 */
@RunWith(Parameterized.class)
public class ColumnMaskWithTypeAlteringFunctionTest extends ColumnMaskTester
{
    /** The column mask as expressed in CQL statements right after the {@code MASKED WITH} keywords. */
    @Parameterized.Parameter
    public String mask;

    /** The type of the masked column. */
    @Parameterized.Parameter(1)
    public CQL3Type.Native type;

    /** The type returned by the tested masking function. */
    @Parameterized.Parameter(2)
    public CQL3Type returnedType;

    private boolean shouldSucceed;
    private String errorMessage;

    @Parameterized.Parameters(name = "mask={0} type={1}")
    public static Collection<Object[]> options()
    {
        return Arrays.asList(new Object[][]{
        { "mask_default()", INT, INT },
        { "mask_default()", TEXT, TEXT },
        { "mask_null()", INT, INT },
        { "mask_null()", TEXT, TEXT },
        { "mask_hash()", INT, BLOB },
        { "mask_hash('SHA-512')", INT, BLOB },
        { "mask_inner(1,2)", TEXT, TEXT },
        { "mask_outer(1,2)", TEXT, TEXT },
        { "mask_replace(0)", INT, INT },
        { "mask_replace(0)", BIGINT, BIGINT },
        { "mask_replace(0)", VARINT, VARINT },
        { "mask_replace('redacted')", ASCII, ASCII },
        { "mask_replace('redacted')", TEXT, TEXT } });
    }

    @Before
    public void before()
    {
        shouldSucceed = returnedType == type;
        errorMessage = shouldSucceed ? null : format("Masking function %s return type is %s.", mask, returnedType);
    }

    @Test
    public void testTypeAlteringFunctionOnCreateTable()
    {
        // Create table with mask
        String table = createTableName();
        String query = format("CREATE TABLE %s.%s (k int PRIMARY KEY, v %s MASKED WITH %s)", KEYSPACE, table, type, mask);
        if (shouldSucceed)
            createTable(query);
        else
            assertThatThrownBy(() -> execute(query)).hasMessageContaining(errorMessage);
    }

    @Test
    public void testTypeAlteringFunctionOnMaskColumn()
    {
        String table = createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", type));
        String query = format("ALTER TABLE %s.%s ALTER v MASKED WITH %s", KEYSPACE, table, mask);
        if (shouldSucceed)
            alterTable(query);
        else
            assertThatThrownBy(() -> execute(query)).hasMessageContaining(errorMessage);
    }

    @Test
    public void testTypeAlteringFunctionOnAddColumn()
    {
        String table = createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", type));
        String query = format("ALTER TABLE %s.%s ADD n %s MASKED WITH %s", KEYSPACE, table, type, mask);
        if (shouldSucceed)
            alterTable(query);
        else
            assertThatThrownBy(() -> execute(query)).hasMessageContaining(errorMessage);
    }
}

