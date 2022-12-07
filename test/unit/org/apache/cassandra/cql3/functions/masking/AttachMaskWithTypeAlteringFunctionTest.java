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
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQL3Type;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link AttachMaskTester} for masks using functions that might return values with a type different to the type of the
 * masked column. Those queries should fail if the return type of the function is indeed different to the type of the
 * masked column.
 */
@RunWith(Parameterized.class)
public class AttachMaskWithTypeAlteringFunctionTest extends AttachMaskTester
{
    /** The column mask as expressed in CQL statements right after the {@code MASKED WITH} keywords. */
    @Parameterized.Parameter
    public String mask;

    /** The CQL data types that are returned by the tested masking function. */
    @Parameterized.Parameter(1)
    public List<CQL3Type> returnedTypes;

    @Parameterized.Parameters(name = "mask={0}")
    public static Collection<Object[]> options()
    {
        return Arrays.asList(new Object[][]{
        { "mask_default()", null },
        { "mask_hash()", singletonList(CQL3Type.Native.BLOB) },
        { "mask_hash('SHA-512')", singletonList(CQL3Type.Native.BLOB) },
        { "mask_inner(1,2)", asList(CQL3Type.Native.TEXT, CQL3Type.Native.VARCHAR) },
        { "mask_outer(1,2)", asList(CQL3Type.Native.TEXT, CQL3Type.Native.VARCHAR) },
        { "mask_replace(1)", singletonList(CQL3Type.Native.INT) },
        { "mask_replace((int)1)", singletonList(CQL3Type.Native.INT) },
        { "mask_replace((bigint)1)", singletonList(CQL3Type.Native.BIGINT) },
        { "mask_replace((text)'redacted')", asList(CQL3Type.Native.TEXT, CQL3Type.Native.VARCHAR) } });
    }

    @Test
    public void testTypeAlteringFunction()
    {
        for (CQL3Type.Native type : CQL3Type.Native.values())
        {
            if (type == CQL3Type.Native.EMPTY)
                continue;

            boolean shouldSucceed = returnedTypes == null || ImmutableSet.copyOf(returnedTypes).contains(type);
            String errorMessage = shouldSucceed
                                  ? null
                                  : format("Masking function %s return type is %s.", mask, returnedTypes.get(0));

            // Create table with mask
            String table = createTableName();
            String createQuery = format("CREATE TABLE %s.%s (k int PRIMARY KEY, v %s MASKED WITH %s)",
                                        KEYSPACE, table, type, mask);
            if (shouldSucceed)
                createTable(createQuery);
            else
                assertThatThrownBy(() -> execute(createQuery)).hasMessageContaining(errorMessage);

            // Alter table with mask
            table = createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v %s)", type));
            String alterQuery = format("ALTER TABLE %s.%s ALTER v MASKED WITH %s", KEYSPACE, table, mask);
            if (shouldSucceed)
                alterTable(alterQuery);
            else
                assertThatThrownBy(() -> execute(alterQuery)).hasMessageContaining(errorMessage);

            // Alter table adding a new column with mask
            String addQuery = format("ALTER TABLE %s.%s ADD n %s MASKED WITH %s", KEYSPACE, table, type, mask);
            if (shouldSucceed)
                alterTable(addQuery);
            else
                assertThatThrownBy(() -> execute(addQuery)).hasMessageContaining(errorMessage);
        }
    }
}

