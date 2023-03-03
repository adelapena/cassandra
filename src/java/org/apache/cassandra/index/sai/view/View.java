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

package org.apache.cassandra.index.sai.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

/**
 * The View is an immutable, point in time, view of the avalailable {@link SSTableIndex}es for an index.
 *
 * The view maintains a {@link RangeTermTree} for querying the view by value range and a {@link IntervalTree}
 * for querying the view by key range. These are used by the {@link org.apache.cassandra.index.sai.plan.QueryController}
 * to select the set of {@link SSTableIndex}es to perform a query without needing to query indexes that
 * are known not to contain to the requested expression value range or do not lie within the requested key range.
 */
public class View implements Iterable<SSTableIndex>
{
    private final Map<Descriptor, SSTableIndex> view;

    private final RangeTermTree rangeTermTree;
    private final AbstractType<?> keyValidator;
    private final IntervalTree<DecoratedKey, SSTableIndex, Interval<DecoratedKey, SSTableIndex>> keyIntervalTree;

    public View(IndexContext context, Collection<SSTableIndex> indexes)
    {
        this.view = new HashMap<>();
        this.keyValidator = context.keyValidator();

        AbstractType<?> termValidator = context.getValidator();

        RangeTermTree.Builder rangeTermTreeBuilder = new RangeTermTree.Builder(termValidator);

        List<Interval<DecoratedKey, SSTableIndex>> keyIntervals = new ArrayList<>();
        for (SSTableIndex sstableIndex : indexes)
        {
            this.view.put(sstableIndex.getSSTable().descriptor, sstableIndex);
            rangeTermTreeBuilder.add(sstableIndex);
            keyIntervals.add(Interval.create(sstableIndex.minKey(), sstableIndex.maxKey(), sstableIndex));
        }

        this.rangeTermTree = rangeTermTreeBuilder.build();
        this.keyIntervalTree = IntervalTree.build(keyIntervals);
    }

    /**
     * Search for a list of {@link SSTableIndex}es that contain values within
     * the value range requested in the {@link Expression}
     */
    public List<SSTableIndex> match(Expression expression)
    {
        return rangeTermTree.search(expression);
    }

    /**
     * Search for a list of {@link SSTableIndex}es that lie within the requested
     * key range.
     */
    public List<SSTableIndex> match(DecoratedKey minKey, DecoratedKey maxKey)
    {
        return keyIntervalTree.search(Interval.create(minKey, maxKey, null));
    }

    @Override
    public Iterator<SSTableIndex> iterator()
    {
        return view.values().iterator();
    }

    public Collection<SSTableIndex> getIndexes()
    {
        return view.values();
    }

    public boolean containsSSTable(SSTableReader sstable)
    {
        return view.containsKey(sstable.descriptor);
    }

    public int size()
    {
        return view.size();
    }

    @Override
    public String toString()
    {
        return String.format("View{view=%s, keyValidator=%s, keyIntervalTree=%s}", view, keyValidator, keyIntervalTree);
    }
}
