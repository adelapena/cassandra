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

package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.disk.IndexSearchResultIterator;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter filterOperation;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter filterOperation,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.filterOperation = filterOperation;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);
    }

    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter filterOperation()
    {
        return this.filterOperation;
    }

    /**
     * @return token ranges used in the read command
     */
    List<DataRange> dataRanges()
    {
        return ranges;
    }

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    /**
     * @return indexed {@code IndexContext} if index is found; otherwise return non-indexed {@code IndexContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        Set<StorageAttachedIndex> indexes = cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class);

        return indexes.isEmpty() ? new IndexContext(cfs.metadata().keyspace,
                                                    cfs.metadata().name,
                                                    cfs.metadata().partitionKeyType,
                                                    cfs.metadata().comparator,
                                                    expression.column(),
                                                    IndexTarget.Type.VALUES,
                                                    null)
                                 : indexes.iterator().next().getIndexContext();
    }

    public UnfilteredRowIterator queryStorage(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        try
        {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     RowFilter.none(),
                                                                                     DataLimits.NONE,
                                                                                     key.partitionKey(),
                                                                                     makeFilter(key));

            return partition.queryMemtableAndDisk(cfs, executionController);
        }
        finally
        {
            queryContext.checkpoint();
        }
    }

    /**
     * Build a {@link KeyRangeIterator.Builder} from the given list of expressions by applying given operation (AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
    public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(expressions.size());

        Set<Map.Entry<Expression, NavigableSet<SSTableIndex>>> view = referenceAndGetView(expressions).entrySet();

        try
        {
            for (Map.Entry<Expression, NavigableSet<SSTableIndex>> e : view)
            {
                @SuppressWarnings({"resource", "RedundantSuppression"}) // RangeIterators are closed by releaseIndexes
                KeyRangeIterator index = IndexSearchResultIterator.build(e.getKey(), e.getValue(), mergeRange, queryContext);

                builder.add(index);
            }
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            builder.cleanup();
            view.forEach(e -> e.getValue().forEach(QueryController::releaseQuietly));
            throw t;
        }
        return builder;
    }

    /**
     * Returns whether this query is not selecting the {@link PrimaryKey}.
     * The query does not select the key if both of the following statements are false:
     *  1. The table associated with the query is not using clustering keys
     *  2. The clustering index filter for the command wants the row.
     *
     *  Item 2 is important in paged queries where the {@link org.apache.cassandra.db.filter.ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is not selected by the query
     */
    public boolean doesNotSelect(PrimaryKey key)
    {
        return !key.hasEmptyClustering() && !command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        if (key.hasEmptyClustering())
            return clusteringIndexFilter;
        else
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), cfs.metadata().comparator),
                                                  clusteringIndexFilter.isReversed());
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(index.getIndexContext().logMessage("Failed to release index on SSTable {}"), index.getSSTable().descriptor, e);
        }
    }

    /**
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    /**
     * Try to reference all SSTableIndexes before querying on disk indexes.
     *
     * If we attempt to proceed into {@link IndexSearchResultIterator#build(Expression, Set, AbstractBounds, QueryContext)}
     * without first referencing all indexes, a concurrent compaction may decrement one or more of their backing
     * SSTable {@link Ref} instances. This will allow the {@link SSTableIndex} itself to be released and will fail the query.
     */
    private Map<Expression, NavigableSet<SSTableIndex>> referenceAndGetView(Collection<Expression> expressions)
    {
        SortedSet<String> indexNames = new TreeSet<>();
        try
        {
            while (true)
            {
                List<SSTableIndex> referencedIndexes = new ArrayList<>();
                boolean failed = false;

                Map<Expression, NavigableSet<SSTableIndex>> view = getView(expressions);

                for (SSTableIndex index : view.values().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                {
                    indexNames.add(index.getIndexContext().getIndexName());

                    if (index.reference())
                    {
                        referencedIndexes.add(index);
                    }
                    else
                    {
                        failed = true;
                        break;
                    }
                }

                if (failed)
                {
                    // TODO: This might be a good candidate for a table/index group metric in the future...
                    referencedIndexes.forEach(QueryController::releaseQuietly);
                }
                else
                {
                    return view;
                }
            }
        }
        finally
        {
            Tracing.trace("Querying storage-attached indexes {}", indexNames);
        }
    }

    private Map<Expression, NavigableSet<SSTableIndex>> getView(Collection<Expression> expressions)
    {
        // first let's determine the primary expression
        Pair<Expression, NavigableSet<SSTableIndex>> primary = calculatePrimary(expressions);

        Map<Expression, NavigableSet<SSTableIndex>> indexes = new HashMap<>();
        for (Expression expression : expressions)
        {
            // Non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (expression.context.isNotIndexed())
            {
                continue;
            }

            // primary expression, we'll have to add as is
            if (primary != null && expression.equals(primary.left))
            {
                indexes.put(primary.left, primary.right);

                continue;
            }

            View view = expression.context.getView();

            NavigableSet<SSTableIndex> readers = new TreeSet<>(SSTableIndex.COMPARATOR);
            if (primary != null)
            {
                for (SSTableIndex index : primary.right)
                    readers.addAll(view.match(index.minKey(), index.maxKey()));
            }
            else
            {
                readers.addAll(applyScope(view.match(expression)));
            }

            indexes.put(expression, readers);
        }

        return indexes;
    }

    // The purpose of this method is to attempt to calculate the most selective expression based on
    // which indexes are within the requested key range and return the expression along with the
    // selected indexes.
    //
    // The result will either be null or will contain, at least, one index.
    private Pair<Expression, NavigableSet<SSTableIndex>> calculatePrimary(Collection<Expression> expressions)
    {
        Expression primaryExpression = null;
        NavigableSet<SSTableIndex> primaryIndexes = null;

        for (Expression expression : expressions)
        {
            if (expression.context.isNotIndexed())
                continue;

            View view = expression.context.getView();

            NavigableSet<SSTableIndex> indexes = new TreeSet<>(SSTableIndex.COMPARATOR);
            indexes.addAll(applyScope(view.match(expression)));

            if (indexes.isEmpty())
                continue;

            if (primaryExpression == null || primaryIndexes.size() > indexes.size())
            {
                primaryIndexes = indexes;
                primaryExpression = expression;
            }
        }

        return primaryExpression == null ? null : Pair.create(primaryExpression, primaryIndexes);
    }

    private List<SSTableIndex> applyScope(List<SSTableIndex> indexes)
    {
        return indexes.stream().filter(this::indexInRange).collect(Collectors.toList());
    }

    private boolean indexInRange(SSTableIndex index)
    {
        SSTableReader sstable = index.getSSTable();
        return mergeRange.left.compareTo(sstable.last) <= 0 &&
               (mergeRange.right.isMinimum() || sstable.first.compareTo(mergeRange.right) <= 0);
    }

    /**
     * Returns the {@link DataRange} list covered by the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the data ranges covered by {@code command}
     */
    private static List<DataRange> dataRanges(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            return Lists.newArrayList(new DataRange(new Range<>(key, key), cmd.clusteringIndexFilter()));
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
            return Lists.newArrayList(cmd.dataRange());
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }
}
