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

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.guardrails.GuardrailEvent.GuardrailEventType;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class GuardrailTester extends CQLTester
{
    // Name used when testing CREATE TABLE that should be aborted (we need to provide it as assertFails, which
    // is used to assert the failure, does not know that it is a CREATE TABLE and would thus reuse the name of the
    // previously created table, which is not what we want).
    protected static final String FAIL_TABLE = "abort_table_creation_test";

    private static final String USERNAME = "guardrail_user";
    private static final String PASSWORD = "guardrail_password";

    protected static ClientState systemClientState, userClientState, superClientState;

    /** The tested guardrail, if we are testing a specific one. */
    @Nullable
    protected final Guardrail guardrail;

    /** A listener for emitted diagnostic events. */
    protected final Listener listener;

    public GuardrailTester()
    {
        this(null);
    }

    public GuardrailTester(@Nullable Guardrail guardrail)
    {
        this.guardrail = guardrail;
        this.listener = new Listener();
    }

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
        guardrails().setEnabled(true);
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);

        systemClientState = ClientState.forInternalCalls();
        userClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 123));
        superClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 321));
        superClientState.login(new AuthenticatedUser(CassandraRoleManager.DEFAULT_SUPERUSER_NAME));
    }

    /**
     * Creates an ordinary user that is not excluded from guardrails, that is, a user that is not super not internal.
     */
    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        useSuperUser();
        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USERNAME, PASSWORD));
        executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, USERNAME));
        useUser(USERNAME, PASSWORD);

        String useKeyspaceQuery = "USE " + keyspace();
        execute(userClientState, useKeyspaceQuery);
        execute(systemClientState, useKeyspaceQuery);
        execute(superClientState, useKeyspaceQuery);

        DiagnosticEventService.instance().subscribe(GuardrailEvent.class, listener);
    }

    @After
    public void afterGuardrailTest() throws Throwable
    {
        DiagnosticEventService.instance().unsubscribe(listener);
    }

    static Guardrails guardrails()
    {
        return Guardrails.instance;
    }

    protected <T> void assertValidProperty(BiConsumer<Guardrails, T> setter, T value)
    {
        setter.accept(guardrails(), value);
    }

    protected <T> void assertInvalidProperty(BiConsumer<Guardrails, T> setter,
                                             T value,
                                             String message,
                                             Object... messageArgs)
    {
        Assertions.assertThatThrownBy(() -> setter.accept(guardrails(), value))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage(format(message, messageArgs));
    }

    @SafeVarargs
    protected final void testExcludedUsers(Supplier<String>... queries) throws Throwable
    {
        execute("USE " + keyspace());
        assertSuperuserIsExcluded(queries);
        assertInternalQueriesAreExcluded(queries);
    }

    @SafeVarargs
    private final void assertInternalQueriesAreExcluded(Supplier<String>... queries) throws Throwable
    {
        for (Supplier<String> query : queries)
        {
            assertValid(() -> execute(systemClientState, query.get()));
        }
    }

    @SafeVarargs
    private final void assertSuperuserIsExcluded(Supplier<String>... queries) throws Throwable
    {
        for (Supplier<String> query : queries)
        {
            assertValid(() -> execute(superClientState, query.get()));
        }
    }

    protected void assertValid(CheckedFunction function) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();
            assertEmptyWarnings();
            listener.assertNotWarned();
            listener.assertNotFailed();
        }
        catch (InvalidRequestException e)
        {
            fail("Expected not to fail, but failed with error message: " + e.getMessage());
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertValid(String query) throws Throwable
    {
        assertValid(() -> execute(userClientState, query));
    }

    protected void assertWarns(CheckedFunction function, String message) throws Throwable
    {
        // We use client warnings to check we properly warn as this is the most convenient. Technically,
        // this doesn't validate we also log the warning, but that's probably fine ...
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();
            assertWarnings(message);
            listener.assertWarned(message);
            listener.assertNotFailed();
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertWarns(String message, String query) throws Throwable
    {
        assertWarns(() -> execute(userClientState, query), message);
    }

    protected void assertFails(CheckedFunction function, String message) throws Throwable
    {
        assertFails(function, message, true);
    }

    protected void assertFails(CheckedFunction function, String message, boolean thrown) throws Throwable
    {
        ClientWarn.instance.captureWarnings();
        try
        {
            function.apply();

            if (thrown)
                fail("Expected to fail, but it did not");
        }
        catch (InvalidRequestException | InvalidQueryException e)
        {
            assertTrue("Expect no exception thrown", thrown);

            assertTrue(format("Full error message '%s' does not contain expected message '%s'", e.getMessage(), message),
                       e.getMessage().contains(message));

            assertWarnings(message);
            listener.assertNotWarned();
            listener.assertFailed(message);
        }
        finally
        {
            ClientWarn.instance.resetWarnings();
            listener.clear();
        }
    }

    protected void assertFails(String message, String query) throws Throwable
    {
        assertFails(() -> execute(userClientState, query), message);
    }

    private void assertWarnings(String message)
    {
        List<String> warnings = getWarnings();

        assertFalse("Expected to warn, but no warning was received", warnings == null || warnings.isEmpty());
        assertEquals(format("Got more thant 1 warning (got %d => %s)", warnings.size(), warnings),
                     1,
                     warnings.size());

        String warning = warnings.get(0);
        assertTrue(format("Warning log message '%s' does not contain expected message '%s'", warning, message),
                   warning.contains(message));
    }

    private void assertEmptyWarnings()
    {
        List<String> warnings = getWarnings();

        if (warnings == null) // will always be the case in practice currently, but being defensive if this change
            warnings = Collections.emptyList();

        assertTrue(format("Expect no warning messages but got %s", warnings), warnings.isEmpty());
    }

    private List<String> getWarnings()
    {
        List<String> warnings = ClientWarn.instance.getWarnings();

        return warnings == null
               ? Collections.emptyList()
               : warnings.stream()
                         .filter(w -> !w.equals(View.USAGE_WARNING) && !w.equals(SASIIndex.USAGE_WARNING))
                         .collect(Collectors.toList());
    }

    protected void assertConfigFails(Consumer<Guardrails> consumer, String message)
    {
        try
        {
            consumer.accept(guardrails());
            fail("Expected failure");
        }
        catch (IllegalArgumentException e)
        {
            String actualMessage = e.getMessage();
            assertTrue(String.format("Failure message '%s' does not contain expected message '%s'", actualMessage, message),
                       actualMessage.contains(message));
        }
    }

    protected ResultMessage execute(ClientState state, String query)
    {
        QueryState queryState = new QueryState(state);

        String formattedQuery = formatQuery(query);
        CQLStatement statement = QueryProcessor.parseStatement(formattedQuery, queryState.getClientState());
        statement.validate(state);

        QueryOptions options = QueryOptions.forInternalCalls(Collections.emptyList());

        return statement.executeLocally(queryState, options);
    }

    /**
     * A listener for guardrails diagnostic events.
     */
    public class Listener implements Consumer<GuardrailEvent>
    {
        private final List<String> warnings = new CopyOnWriteArrayList<>();
        private final List<String> failures = new CopyOnWriteArrayList<>();

        @Override
        public void accept(GuardrailEvent event)
        {
            assertNotNull(event);
            Map<String, Serializable> map = event.toMap();

            if (guardrail != null)
                assertEquals(guardrail.name, map.get("name"));

            GuardrailEventType type = (GuardrailEventType) event.getType();
            String message = map.toString();

            switch (type)
            {
                case WARNED:
                    warnings.add(message);
                    break;
                case FAILED:
                    failures.add(message);
                    break;
                default:
                    fail("Unexpected diagnostic event:" + type);
            }
        }

        public void clear()
        {
            warnings.clear();
            failures.clear();
        }

        public void assertNotWarned()
        {
            assertTrue(format("Expect no warning diagnostic events but got %s", warnings), warnings.isEmpty());
        }

        public void assertWarned(String message)
        {
            assertFalse("Expected to emit warning diagnostic event, but no warning was emitted", warnings.isEmpty());
            assertEquals(format("Got more thant 1 warning diagnostic event (got %d => %s)", warnings.size(), warnings),
                         1, warnings.size());

            String warning = warnings.get(0);
            assertTrue(format("Warning diagnostic event '%s' does not contain expected message '%s'", warning, message),
                       warning.contains(message));
        }

        public void assertNotFailed()
        {
            assertTrue(format("Expect no failure diagnostic events but got %s", failures), failures.isEmpty());
        }

        public void assertFailed(String message)
        {
            assertFalse("Expected to emit failure diagnostic event, but no failure was emitted", failures.isEmpty());
            assertEquals(format("Got more thant 1 failure diagnostic event (got %d => %s)", failures.size(), failures),
                         1, failures.size());

            String failure = failures.get(0);
            assertTrue(format("Failure diagnostic event '%s' does not contain expected message '%s'", failure, message),
                       failure.contains(message));
        }
    }
}
