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

package org.apache.cassandra.db.guardrails;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;

import static java.lang.String.format;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.db.ConsistencyLevel.ANY;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_SERIAL;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;
import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

/**
 * Tests the guardrail for table properties, {@link Guardrails#writeConsistencyLevels}.
 */
public class GuardrailWriteConsistencyLevelsTest extends GuardrailTester
{
    private static final String WARNED_PROPERTY_NAME = "write_consistency_levels_warned";
    private static final String DISALLOWED_PROPERTY_NAME = "write_consistency_levels_disallowed";

    @Before
    public void before()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");

        warnConsistencyLevels();
        disableConsistencyLevels();
    }

    @Test
    public void testConfigValidation()
    {
        String message = "Invalid value for %s: null is not allowed";
        assertInvalidProperty(Guardrails::setWriteConsistencyLevelsWarned, null, message, WARNED_PROPERTY_NAME);
        assertInvalidProperty(Guardrails::setWriteConsistencyLevelsDisallowed, null, message, DISALLOWED_PROPERTY_NAME);

        assertValidProperty(Collections.emptySet());
        assertValidProperty(EnumSet.allOf(ConsistencyLevel.class));

        assertValidPropertyCSV("");
        assertValidPropertyCSV(EnumSet.allOf(ConsistencyLevel.class).stream().map(ConsistencyLevel::toString).collect(Collectors.joining(",")));

        assertInvalidPropertyCSV("invalid", "INVALID");
        assertInvalidPropertyCSV("ONE,invalid1,invalid2", "INVALID1");
        assertInvalidPropertyCSV("invalid1,invalid2,ONE", "INVALID1");
        assertInvalidPropertyCSV("invalid1,ONE,invalid2", "INVALID1");
    }

    private void assertValidProperty(Set<ConsistencyLevel> properties)
    {
        assertValidProperty(Guardrails::setWriteConsistencyLevelsWarned, Guardrails::getWriteConsistencyLevelsWarned, properties);
        assertValidProperty(Guardrails::setWriteConsistencyLevelsDisallowed, Guardrails::getWriteConsistencyLevelsDisallowed, properties);
    }

    private void assertValidPropertyCSV(String csv)
    {
        csv = sortCSV(csv);
        assertValidProperty(Guardrails::setWriteConsistencyLevelsWarnedCSV, g -> sortCSV(g.getWriteConsistencyLevelsWarnedCSV()), csv);
        assertValidProperty(Guardrails::setWriteConsistencyLevelsDisallowedCSV, g -> sortCSV(g.getWriteConsistencyLevelsDisallowedCSV()), csv);
    }

    private void assertInvalidPropertyCSV(String properties, String rejected)
    {
        String message = "No enum constant org.apache.cassandra.db.ConsistencyLevel.%s";
        assertInvalidProperty(Guardrails::setWriteConsistencyLevelsWarnedCSV, properties, message, rejected);
        assertInvalidProperty(Guardrails::setWriteConsistencyLevelsDisallowedCSV, properties, message, rejected);
    }

    @Test
    public void testInsert() throws Throwable
    {
        testQuery("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val')");
        testLWTQuery("INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS");
    }

    @Test
    public void testUpdate() throws Throwable
    {
        testQuery("UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2");
        testLWTQuery("UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 IF EXISTS");
    }

    @Test
    public void testDelete() throws Throwable
    {
        testQuery("DELETE FROM %s WHERE k=1");
        testLWTQuery("DELETE FROM %s WHERE k=1 AND c=2 IF EXISTS");
    }

    @Test
    public void testBatch() throws Throwable
    {
        testQuery("BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') APPLY BATCH");
        testQuery("BEGIN BATCH UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 APPLY BATCH");
        testQuery("BEGIN BATCH DELETE FROM %s WHERE k=1 APPLY BATCH");

        testLWTQuery("BEGIN BATCH INSERT INTO %s (k, c, v) VALUES (1, 2, 'val') IF NOT EXISTS APPLY BATCH");
        testLWTQuery("BEGIN BATCH UPDATE %s SET v = 'val2' WHERE k = 1 AND c = 2 IF EXISTS APPLY BATCH");
        testLWTQuery("BEGIN BATCH DELETE FROM %s WHERE k=1 AND c=2 IF EXISTS APPLY BATCH");
    }

    private void testQuery(String query) throws Throwable
    {
        testQuery(query, ONE);
        testQuery(query, ALL);
        testQuery(query, ANY);
        testQuery(query, QUORUM);
        testQuery(query, LOCAL_ONE);
        testQuery(query, LOCAL_QUORUM);
    }

    private void testQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        warnConsistencyLevels();
        disableConsistencyLevels();
        assertValid(query, cl, null);

        warnConsistencyLevels(cl);
        assertWarns(query, cl, null, cl);

        warnConsistencyLevels();
        disableConsistencyLevels(cl);
        assertAborts(query, cl, null, cl);
    }

    private void testLWTQuery(String query) throws Throwable
    {
        testLWTQuery(query, ONE);
        testLWTQuery(query, ALL);
        testLWTQuery(query, QUORUM);
        testLWTQuery(query, LOCAL_ONE);
        testLWTQuery(query, LOCAL_QUORUM);
    }

    private void testLWTQuery(String query, ConsistencyLevel cl) throws Throwable
    {
        disableConsistencyLevels();

        warnConsistencyLevels();
        assertValid(query, cl, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertValid(query, cl, null);

        warnConsistencyLevels(cl);
        assertWarns(query, cl, SERIAL, cl);
        assertWarns(query, cl, LOCAL_SERIAL, cl);
        assertWarns(query, cl, null, cl);

        warnConsistencyLevels(SERIAL);
        assertWarns(query, cl, SERIAL, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertWarns(query, cl, null, SERIAL);

        warnConsistencyLevels(LOCAL_SERIAL);
        assertValid(query, cl, SERIAL);
        assertWarns(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertValid(query, cl, null);

        warnConsistencyLevels(SERIAL, LOCAL_SERIAL);
        assertWarns(query, cl, SERIAL, SERIAL);
        assertWarns(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertWarns(query, cl, null, SERIAL);

        warnConsistencyLevels();

        disableConsistencyLevels(cl);
        assertAborts(query, cl, SERIAL, cl);
        assertAborts(query, cl, LOCAL_SERIAL, cl);
        assertAborts(query, cl, null, cl);

        disableConsistencyLevels(SERIAL);
        assertAborts(query, cl, SERIAL, SERIAL);
        assertValid(query, cl, LOCAL_SERIAL);
        assertAborts(query, cl, null, SERIAL);

        disableConsistencyLevels(LOCAL_SERIAL);
        assertValid(query, cl, SERIAL);
        assertAborts(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertValid(query, cl, null);

        disableConsistencyLevels(SERIAL, LOCAL_SERIAL);
        assertAborts(query, cl, SERIAL, SERIAL);
        assertAborts(query, cl, LOCAL_SERIAL, LOCAL_SERIAL);
        assertAborts(query, cl, null, SERIAL);
    }

    private void warnConsistencyLevels(ConsistencyLevel... consistencyLevels)
    {
        guardrails().setWriteConsistencyLevelsWarned(ImmutableSet.copyOf(consistencyLevels));
    }

    private void disableConsistencyLevels(ConsistencyLevel... consistencyLevels)
    {
        guardrails().setWriteConsistencyLevelsDisallowed(ImmutableSet.copyOf(consistencyLevels));
    }

    private void assertValid(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(userClientState, query, cl, serialCl));
    }

    private void assertWarns(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel warnedCl) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not recommended for write consistency levels (warned values are: %s)",
                           warnedCl, guardrails().getWriteConsistencyLevelsWarned()));

        assertExcludedUsers(query, cl, serialCl);
    }

    private void assertAborts(String query, ConsistencyLevel cl, ConsistencyLevel serialCl, ConsistencyLevel rejectedCl) throws Throwable
    {
        assertFails(() -> execute(userClientState, query, cl, serialCl),
                    format("Provided values [%s] are not allowed for write consistency levels (disallowed values are: %s)",
                           rejectedCl, guardrails().getWriteConsistencyLevelsDisallowed()));

        assertExcludedUsers(query, cl, serialCl);
    }

    private void assertExcludedUsers(String query, ConsistencyLevel cl, ConsistencyLevel serialCl) throws Throwable
    {
        assertValid(() -> execute(superClientState, query, cl, serialCl));
        assertValid(() -> execute(systemClientState, query, cl, serialCl));
    }
}
