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
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IColumnIteratorFactory;
import org.apache.cassandra.db.columniterator.LazyColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.compaction.ICompactionScanner;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SSTableScanner implements ICompactionScanner
{
    protected final RandomAccessReader dfile;
    protected final RandomAccessReader ifile;
    public final SSTableReader sstable;

    private final Iterator<AbstractBounds<RowPosition>> rangeIterator;
    private AbstractBounds<RowPosition> currentRange;

    private final DataRange dataRange;

    protected Iterator<OnDiskAtomIterator> iterator;

    /**
     * @param sstable SSTable to scan; must not be null
     * @param dataRange a single range to scan; must not be null
     * @param limiter background i/o RateLimiter; may be null
     */
    SSTableScanner(SSTableReader sstable, DataRange dataRange, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = dataRange;

        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(2);
        if (dataRange.isWrapAround() && !dataRange.stopKey().isMinimum(sstable.partitioner))
        {
            // split the wrapping range into two parts: 1) the part that starts at the beginning of the sstable, and
            // 2) the part that comes before the wrap-around
            boundsList.add(new Bounds<>(sstable.partitioner.getMinimumToken().minKeyBound(), dataRange.stopKey(), sstable.partitioner));
            boundsList.add(new Bounds<>(dataRange.startKey(), sstable.partitioner.getMinimumToken().maxKeyBound(), sstable.partitioner));
        }
        else
        {
            boundsList.add(new Bounds<>(dataRange.startKey(), dataRange.stopKey(), sstable.partitioner));
        }
        this.rangeIterator = boundsList.iterator();
    }

    /**
     * @param sstable SSTable to scan; must not be null
     * @param tokenRanges A set of token ranges to scan
     * @param limiter background i/o RateLimiter; may be null
     */
    SSTableScanner(SSTableReader sstable, Collection<Range<Token>> tokenRanges, RateLimiter limiter)
    {
        assert sstable != null;

        this.dfile = limiter == null ? sstable.openDataReader() : sstable.openDataReader(limiter);
        this.ifile = sstable.openIndexReader();
        this.sstable = sstable;
        this.dataRange = null;

        List<Range<Token>> normalized = Range.normalize(tokenRanges);
        List<AbstractBounds<RowPosition>> boundsList = new ArrayList<>(normalized.size());
        for (Range<Token> range : normalized)
            boundsList.add(new Range<RowPosition>(range.left.maxKeyBound(sstable.partitioner),
                                                  range.right.maxKeyBound(sstable.partitioner),
                                                  sstable.partitioner));

        this.rangeIterator = boundsList.iterator();
    }

    private void seekToCurrentRangeStart()
    {
        if (currentRange.left.isMinimum(sstable.partitioner))
            return;

        long indexPosition = sstable.getIndexScanPosition(currentRange.left);
        // -1 means the key is before everything in the sstable. So just start from the beginning.
        if (indexPosition == -1)
        {
            // Note: this method shouldn't assume we're at the start of the sstable already (see #6638) and
            // the seeks are no-op anyway if we are.
            ifile.seek(0);
            dfile.seek(0);
            return;
        }

        ifile.seek(indexPosition);
        try
        {

            while (!ifile.isEOF())
            {
                indexPosition = ifile.getFilePointer();
                DecoratedKey indexDecoratedKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                int comparison = indexDecoratedKey.compareTo(currentRange.left);
                // because our range start may be inclusive or exclusive, we need to also contains()
                // instead of just checking (comparison >= 0)
                if (comparison > 0 || currentRange.contains(indexDecoratedKey))
                {
                    // Found, just read the dataPosition and seek into index and data files
                    long dataPosition = ifile.readLong();
                    ifile.seek(indexPosition);
                    dfile.seek(dataPosition);
                    break;
                }
                else
                {
                    RowIndexEntry.serializer.skip(ifile);
                }
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, sstable.getFilename());
        }
    }

    public void close() throws IOException
    {
        FileUtils.close(dfile, ifile);
    }

    public long getLengthInBytes()
    {
        return dfile.length();
    }

    public long getCurrentPosition()
    {
        return dfile.getFilePointer();
    }

    public String getBackingFiles()
    {
        return sstable.toString();
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.hasNext();
    }

    public OnDiskAtomIterator next()
    {
        if (iterator == null)
            iterator = createIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private Iterator<OnDiskAtomIterator> createIterator()
    {
        return new KeyScanningIterator();
    }

    protected class KeyScanningIterator extends AbstractIterator<OnDiskAtomIterator>
    {
        private DecoratedKey nextKey;
        private RowIndexEntry nextEntry;
        private DecoratedKey currentKey;
        private RowIndexEntry currentEntry;

        protected OnDiskAtomIterator computeNext()
        {
            try
            {
                if (nextEntry == null)
                {
                    do
                    {
                        // we're starting the first range or we just passed the end of the previous range
                        if (!rangeIterator.hasNext())
                            return endOfData();

                        currentRange = rangeIterator.next();
                        seekToCurrentRangeStart();

                        if (ifile.isEOF())
                            return endOfData();

                        currentKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                        currentEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                    } while (!currentRange.contains(currentKey));
                }
                else
                {
                    // we're in the middle of a range
                    currentKey = nextKey;
                    currentEntry = nextEntry;
                }

                long readEnd;
                if (ifile.isEOF())
                {
                    nextEntry = null;
                    nextKey = null;
                    readEnd = dfile.length();
                }
                else
                {
                    // we need the position of the start of the next key, regardless of whether it falls in the current range
                    nextKey = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(ifile));
                    nextEntry = RowIndexEntry.serializer.deserialize(ifile, sstable.descriptor.version);
                    readEnd = nextEntry.position;

                    if (!currentRange.contains(nextKey))
                    {
                        nextKey = null;
                        nextEntry = null;
                    }
                }

                if (dataRange == null || dataRange.selectsFullRowFor(currentKey.key))
                {
                    dfile.seek(currentEntry.position);
                    ByteBufferUtil.readWithShortLength(dfile); // key
                    if (sstable.descriptor.version.hasRowSizeAndColumnCount)
                        dfile.readLong();
                    long dataSize = readEnd - dfile.getFilePointer();
                    return new SSTableIdentityIterator(sstable, dfile, currentKey, dataSize);
                }

                return new LazyColumnIterator(currentKey, new IColumnIteratorFactory()
                {
                    public OnDiskAtomIterator create()
                    {
                        return dataRange.columnFilter(currentKey.key).getSSTableColumnIterator(sstable, dfile, currentKey, currentEntry);
                    }
                });

            }
            catch (IOException e)
            {
                sstable.markSuspect();
                throw new CorruptSSTableException(e, sstable.getFilename());
            }
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "dfile=" + dfile +
               " ifile=" + ifile +
               " sstable=" + sstable +
               ")";
    }
}
