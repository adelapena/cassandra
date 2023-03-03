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

package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.KeyRangeIterator;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A reference-counted container of a {@link SSTableReader} for each column index that:
 * <ul>
 *     <li>Manages references to the SSTable for each query</li>
 *     <li>Exposes a version agnostic searcher onto the column index</li>
 *     <li>Exposes the index metadata for the column index</li>
 * </ul>
 */
public class SSTableIndex
{
    // sort sstable indexes by first key, then last key, then descriptor id
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.id, SSTableIdFactory.COMPARATOR);

    private final SSTableContext sstableContext;
    private final IndexContext indexContext;
    private final Searcher searcher;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        assert indexContext.getValidator() != null;

        this.searcher = sstableContext.indexDescriptor.newSSTableIndexSearcher(sstableContext, indexContext);

        this.sstableContext = sstableContext.sharedCopy(); // this line must not be before any code that may throw
        this.indexContext = indexContext;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public long indexFileCacheSize()
    {
        return searcher.indexFileCacheSize();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return searcher.getRowCount();
    }

    /**
     * @return total size of per-column index components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return sstableContext.indexDescriptor.sizeOnDiskOfPerIndexComponents(indexContext);
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return searcher.minSSTableRowId();
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return searcher.maxSSTableRowId();
    }

    public ByteBuffer minTerm()
    {
        return searcher.minTerm();
    }

    public ByteBuffer maxTerm()
    {
        return searcher.maxTerm();
    }

    public DecoratedKey minKey()
    {
        return searcher.minKey();
    }

    public DecoratedKey maxKey()
    {
        return searcher.maxKey();
    }

    public List<KeyRangeIterator> search(Expression expression,
                                         AbstractBounds<PartitionPosition> keyRange,
                                         SSTableQueryContext context) throws IOException
    {
        return searcher.search(expression, keyRange, context);
    }

    public void populateSegmentView(SimpleDataSet dataSet)
    {
        searcher.populateSystemView(dataSet, sstableContext.sstable);
    }

    public Version getVersion()
    {
        return sstableContext.indexDescriptor.version;
    }

    public SSTableReader getSSTable()
    {
        return sstableContext.sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public boolean isReleased()
    {
        return references.get() <= 0;
    }

    public void release()
    {
        int n = references.decrementAndGet();

        if (n == 0)
        {
            FileUtils.closeQuietly(searcher);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild.
             */
            if (obsolete.get())
            {
                sstableContext.indexDescriptor.deleteColumnIndex(indexContext);
            }
        }
    }

    public void markObsolete()
    {
        obsolete.getAndSet(true);
        release();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableIndex other = (SSTableIndex)o;
        return Objects.equal(sstableContext, other.sstableContext) && Objects.equal(indexContext, other.indexContext);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstableContext, indexContext);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", indexContext.getColumnName())
                          .add("sstable", sstableContext.sstable.descriptor)
                          .add("totalRows", sstableContext.sstable.getTotalRows())
                          .toString();
    }

    /**
     * This is used to abstract the index search between on-disk versions.
     * Callers to this interface should be unaware of the on-disk version for
     * the index.
     *
     * It is responsible for supplying metadata about the on-disk index. This is
     * used during query time to help coordinate queries and is also returned
     * by the virtual tables.
     */
    public interface Searcher extends Closeable
    {
        /**
         * Returns the amount of memory occupied by the index when it is initially loaded.
         * This is the amount of data loaded into internal memory buffers by the index and
         * does include the class footprint overhead. It used by the index metrics.
         */
        long indexFileCacheSize();

        /**
         * Returns the number of indexed rows in the index. This comes from the index
         * metadata created when the index was written and is used by the index metrics.
         */
        long getRowCount();

        /**
         * Returns the minimum indexed rowId for the index. This comes from the index
         * metadata created when the index was written and is used by the index metrics.
         */
        long minSSTableRowId();

        /**
         * Returns the maximum indexed rowId for the index. This comes from the index
         * metadata created when the index was written and is used by the index metrics.
         */
        long maxSSTableRowId();

        /**
         * Returns the minimum term held in the index based on the natural sort order of
         * the index column type comparator. It comes from the index metadata created when
         * the index was written and is used by the index metrics and used in queries to
         * determine whether a term, or range or terms, exists in the index.
         */
        ByteBuffer minTerm();

        /**
         * Returns the maximum term held in the index based on the natural sort order of
         * the index column type comparator. It comes from the index metadata created when
         * the index was written and is used by the index metrics and used in queries to
         * determine whether a term, or range or terms, exists in the index.
         */
        ByteBuffer maxTerm();

        /**
         * Returns the minimum key held in the index. It comes from the index metadata
         * created when the index was written and is used by the index metrics and used
         * in queries to determine whether a query key range is served by the index.
         */
        DecoratedKey minKey();

        /**
         * Returns the maximum key held in the index. It comes from the index metadata
         * created when the index was written and is used by the index metrics and used
         * in queries to determine whether a query key range is served by the index.
         */
        DecoratedKey maxKey();

        /**
         * Perform a search on the index for a single expression and keyRange.
         *
         * The result is a {@link List} of {@link KeyRangeIterator} because there will
         * be a {@link KeyRangeIterator} for each segment in the index. The result
         * will never be null but may be an empty {@link List}.
         *
         * @param expression The {@link Expression} to be searched for
         * @param keyRange The {@link AbstractBounds<PartitionPosition>} defining the
         *                 token range for the search
         * @param context The {@link SSTableQueryContext} holding the per-query state
         * @return a {@link List} of {@link KeyRangeIterator}s containing the results
         * of the search
         */
        List<KeyRangeIterator> search(Expression expression,
                                      AbstractBounds<PartitionPosition> keyRange,
                                      SSTableQueryContext context) throws IOException;

        /**
         * Populates a virtual table using the index metadata owned by the index
         */
        void populateSystemView(SimpleDataSet dataSet, SSTableReader sstable);
    }
}
