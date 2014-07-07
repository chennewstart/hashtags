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
package org.apache.cassandra.db.index.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    @Override
    public List<Row> search(ExtendedFilter filter)
    {
        assert filter.getClause() != null && !filter.getClause().isEmpty();
        return baseCfs.filter(getIndexedIterator(filter), filter);
    }

    private ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final ExtendedFilter filter)
    {

        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        final IndexExpression primary = highestSelectivityPredicate(filter.getClause());
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column_name);
        assert index != null;
        assert index.getIndexCfs() != null;
        final DecoratedKey indexKey = index.getIndexKeyFor(primary.value);

        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}",
                         ((AbstractSimplePerColumnSecondaryIndex) index).expressionString(primary));

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the small
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        final AbstractBounds<RowPosition> range = filter.dataRange.keyRange();
        final ByteBuffer startKey = range.left instanceof DecoratedKey ? ((DecoratedKey)range.left).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        final ByteBuffer endKey = range.right instanceof DecoratedKey ? ((DecoratedKey)range.right).key : ByteBufferUtil.EMPTY_BYTE_BUFFER;

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            private ByteBuffer lastSeenKey = startKey;
            private Iterator<Column> indexColumns;
            private int columnsRead = Integer.MAX_VALUE;

            protected Row computeNext()
            {
                int meanColumns = Math.max(index.getIndexCfs().getMeanColumns(), 1);
                // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
                int rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);
                while (true)
                {
                    if (indexColumns == null || !indexColumns.hasNext())
                    {
                        if (columnsRead < rowsPerQuery)
                        {
                            logger.trace("Read only {} (< {}) last page through, must be done", columnsRead, rowsPerQuery);
                            return endOfData();
                        }

                        if (logger.isTraceEnabled() && (index instanceof AbstractSimplePerColumnSecondaryIndex))
                            logger.trace("Scanning index {} starting with {}",
                                         ((AbstractSimplePerColumnSecondaryIndex)index).expressionString(primary), index.getBaseCfs().metadata.getKeyValidator().getString(startKey));

                        QueryFilter indexFilter = QueryFilter.getSliceFilter(indexKey,
                                                                             index.getIndexCfs().name,
                                                                             lastSeenKey,
                                                                             endKey,
                                                                             false,
                                                                             rowsPerQuery,
                                                                             filter.timestamp);
                        ColumnFamily indexRow = index.getIndexCfs().getColumnFamily(indexFilter);
                        logger.trace("fetched {}", indexRow);
                        if (indexRow == null)
                        {
                            logger.trace("no data, all done");
                            return endOfData();
                        }

                        Collection<Column> sortedColumns = indexRow.getSortedColumns();
                        columnsRead = sortedColumns.size();
                        indexColumns = sortedColumns.iterator();
                        Column firstColumn = sortedColumns.iterator().next();

                        // Paging is racy, so it is possible the first column of a page is not the last seen one.
                        if (lastSeenKey != startKey && lastSeenKey.equals(firstColumn.name()))
                        {
                            // skip the row we already saw w/ the last page of results
                            indexColumns.next();
                            logger.trace("Skipping {}", baseCfs.metadata.getKeyValidator().getString(firstColumn.name()));
                        }
                        else if (range instanceof Range && indexColumns.hasNext() && firstColumn.name().equals(startKey))
                        {
                            // skip key excluded by range
                            indexColumns.next();
                            logger.trace("Skipping first key as range excludes it");
                        }
                    }

                    while (indexColumns.hasNext())
                    {
                        Column column = indexColumns.next();
                        lastSeenKey = column.name();
                        if (column.isMarkedForDelete(filter.timestamp))
                        {
                            logger.trace("skipping {}", column.name());
                            continue;
                        }

                        DecoratedKey dk = baseCfs.partitioner.decorateKey(lastSeenKey);
                        if (!range.right.isMinimum(baseCfs.partitioner) && range.right.compareTo(dk) < 0)
                        {
                            logger.trace("Reached end of assigned scan range");
                            return endOfData();
                        }
                        if (!range.contains(dk))
                        {
                            logger.trace("Skipping entry {} outside of assigned scan range", dk.token);
                            continue;
                        }

                        logger.trace("Returning index hit for {}", dk);
                        ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, filter.columnFilter(lastSeenKey), filter.timestamp));
                        // While the column family we'll get in the end should contains the primary clause column, the initialFilter may not have found it and can thus be null
                        if (data == null)
                            data = TreeMapBackedSortedColumns.factory.create(baseCfs.metadata);

                        // as in CFS.filter - extend the filter to ensure we include the columns
                        // from the index expressions, just in case they weren't included in the initialFilter
                        IDiskAtomFilter extraFilter = filter.getExtraFilter(dk, data);
                        if (extraFilter != null)
                        {
                            ColumnFamily cf = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, extraFilter, filter.timestamp));
                            if (cf != null)
                                data.addAll(cf, HeapAllocator.instance);
                        }

                        if (((KeysIndex)index).isIndexEntryStale(indexKey.key, data, filter.timestamp))
                        {
                            // delete the index entry w/ its own timestamp
                            Column dummyColumn = new Column(primary.column_name, indexKey.key, column.timestamp());
                            ((PerColumnSecondaryIndex)index).delete(dk.key, dummyColumn);
                            continue;
                        }
                        return new Row(dk, data);
                    }
                 }
             }

            public void close() throws IOException {}
        };
    }
}
