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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReplayer;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ReplayRaceTest
{
    private static final String KEYSPACE = "replay_race";
    private static final String TABLE = "test_table";

    @Before
    public void setUp() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1));
    }

    @Test
    public void testRace() throws Exception
    {
        Keyspace ks = Keyspace.open(KEYSPACE);

        // 100 mutationed sorted by its write time
        // The first mutation creates the table
        // Following mutations insert rows into the table
        // When replaying, we should see 99 rows. But, in fact, we will only see 0 rows.
        // In other words, data loss.
        List<Mutation> timeOrderedmnutations = new ArrayList<>(100);
        timeOrderedmnutations.add(schemaChangeToAddTable());
        MigrationManager.announceNewTable(TableMetadata.builder(KEYSPACE, TABLE)
                                                       .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                       .addRegularColumn("val", UTF8Type.instance)
                                                       .build(), true);
        for (int i = 1; i < 100; i ++)
        {
            timeOrderedmnutations.add(new RowUpdateBuilder(ks.getColumnFamilyStore(TABLE).metadata(),
                                               System.currentTimeMillis(), 0, "key_" + i)
                          .add("val", "col_val_" + i)
                          .build());
        }
        Schema.instance.load(Schema.instance.getKeyspaceMetadata(KEYSPACE).withSwapped(Tables.none()));
        assertNull(Schema.instance.getTableMetadata(KEYSPACE, TABLE));

        CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator();
        CommitLogReplayer replayer = new MockReplayer();
        List<Future<?>> futures = timeOrderedmnutations.stream().map(m -> {
            return mutationInitiator.initiateMutation(m, 1, 1, 1, replayer);
        }).collect(Collectors.toList());
        FBUtilities.waitOnFutures(futures);

        List<FilteredPartition> replayed = Util.getAll(Util.cmd(ks.getColumnFamilyStore(TABLE)).build());

        assertEquals(99, replayed.size());
    }

    private Mutation schemaChangeToAddTable() throws UnknownHostException
    {
        CreateTableStatement.Raw raw = new CreateTableStatement.Raw(new QualifiedName(KEYSPACE, TABLE), false);
        raw.setPartitionKeyColumn(ColumnIdentifier.getInterned("pk", false));
        raw.addColumn(ColumnIdentifier.getInterned("pk", false), CQL3Type.Raw.from(CQL3Type.Native.TEXT), false);
        raw.addColumn(ColumnIdentifier.getInterned("val", false), CQL3Type.Raw.from(CQL3Type.Native.TEXT), false);
        CreateTableStatement createTableStatement = raw.prepare(null);
        Keyspaces keyspaces = Schema.instance.snapshot();
        Keyspaces after = createTableStatement.apply(keyspaces);
        Collection<Mutation> schemaChange = SchemaKeyspace.convertSchemaDiffToMutations(Keyspaces.diff(keyspaces, after), TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));
        return schemaChange.iterator().next();
    }

    static class MockReplayer extends CommitLogReplayer
    {
        public MockReplayer()
        {
            super(CommitLog.instance, null, Map.of(), new AlwaysReplayFilter());
        }

        @Override
        public boolean shouldReplay(TableId tableId, CommitLogPosition position)
        {
            return true;
        }
    }
}
