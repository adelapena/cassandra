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

package org.apache.cassandra.test.microbench;


import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class FetchedCellIsQueriedBench
{
    private ColumnDefinition column;
    private ColumnFilter filter;
    private CellPath path;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CFMetaData metadata = CFMetaData.Builder.create("ks", "table")
                                                .withPartitioner(Murmur3Partitioner.instance)
                                                .addPartitionKey("k", Int32Type.instance)
                                                .addRegularColumn("complex", SetType.getInstance(Int32Type.instance, true))
                                                .build();

        column = metadata.getColumnDefinition(ByteBufferUtil.bytes("complex"));
        filter = ColumnFilter.selection(metadata, PartitionColumns.builder().add(column).build());
        path = CellPath.create(ByteBufferUtil.bytes(0));
    }

    @Benchmark
    public Object testNewImpl()
    {
        return filter.fetchedCellIsQueried(column, path);
    }

    @Benchmark
    public Object testOldImpl()
    {
        return filter.fetchedCellIsQueriedOld(column, path);
    }
}
