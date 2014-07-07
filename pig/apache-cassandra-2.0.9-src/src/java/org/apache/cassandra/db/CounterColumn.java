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
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.*;

/**
 * A column that represents a partitioned counter.
 */
public class CounterColumn extends Column
{
    private static final Logger logger = LoggerFactory.getLogger(CounterColumn.class);

    protected static final CounterContext contextManager = CounterContext.instance();

    private final long timestampOfLastDelete;

    public CounterColumn(ByteBuffer name, long value, long timestamp)
    {
        this(name, contextManager.createLocal(value, HeapAllocator.instance), timestamp);
    }

    public CounterColumn(ByteBuffer name, long value, long timestamp, long timestampOfLastDelete)
    {
        this(name, contextManager.createLocal(value, HeapAllocator.instance), timestamp, timestampOfLastDelete);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE);
    }

    public CounterColumn(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        super(name, value, timestamp);
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    public static CounterColumn create(ByteBuffer name, ByteBuffer value, long timestamp, long timestampOfLastDelete, ColumnSerializer.Flag flag)
    {
        if (flag == ColumnSerializer.Flag.FROM_REMOTE || (flag == ColumnSerializer.Flag.LOCAL && contextManager.shouldClearLocal(value)))
            value = contextManager.clearAllLocal(value);
        return new CounterColumn(name, value, timestamp, timestampOfLastDelete);
    }

    @Override
    public Column withUpdatedName(ByteBuffer newName)
    {
        return new CounterColumn(newName, value, timestamp, timestampOfLastDelete);
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    public long total()
    {
        return contextManager.total(value);
    }

    @Override
    public int dataSize()
    {
        /*
         * A counter column adds to a Column :
         *  + 8 bytes for timestampOfLastDelete
         */
        return super.dataSize() + TypeSizes.NATIVE.sizeof(timestampOfLastDelete);
    }

    @Override
    public int serializedSize(TypeSizes typeSizes)
    {
        return super.serializedSize(typeSizes) + typeSizes.sizeof(timestampOfLastDelete);
    }

    @Override
    public Column diff(Column column)
    {
        assert (column instanceof CounterColumn) || (column instanceof DeletedColumn) : "Wrong class type: " + column.getClass();

        if (timestamp() < column.timestamp())
            return column;

        // Note that if at that point, column can't be a tombstone. Indeed,
        // column is the result of merging us with other nodes results, and
        // merging a CounterColumn with a tombstone never return a tombstone
        // unless that tombstone timestamp is greater that the CounterColumn
        // one.
        assert !(column instanceof DeletedColumn) : "Wrong class type: " + column.getClass();

        if (timestampOfLastDelete() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        ContextRelationship rel = contextManager.diff(column.value(), value());
        if (ContextRelationship.GREATER_THAN == rel || ContextRelationship.DISJOINT == rel)
            return column;
        return null;
    }

    /*
     * We have to special case digest creation for counter column because
     * we don't want to include the information about which shard of the
     * context is a delta or not, since this information differs from node to
     * node.
     */
    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
            buffer.writeLong(timestampOfLastDelete);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public Column reconcile(Column column, Allocator allocator)
    {
        assert (column instanceof CounterColumn) || (column instanceof DeletedColumn) : "Wrong class type: " + column.getClass();

        // live + tombstone: track last tombstone
        if (column.isMarkedForDelete(Long.MIN_VALUE)) // cannot be an expired column, so the current time is irrelevant
        {
            // live < tombstone
            if (timestamp() < column.timestamp())
            {
                return column;
            }
            // live last delete >= tombstone
            if (timestampOfLastDelete() >= column.timestamp())
            {
                return this;
            }
            // live last delete < tombstone
            return new CounterColumn(name(), value(), timestamp(), column.timestamp());
        }
        // live < live last delete
        if (timestamp() < ((CounterColumn)column).timestampOfLastDelete())
            return column;
        // live last delete > live
        if (timestampOfLastDelete() > column.timestamp())
            return this;
        // live + live: merge clocks; update value
        return new CounterColumn(
            name(),
            contextManager.merge(value(), column.value(), allocator),
            Math.max(timestamp(), column.timestamp()),
            Math.max(timestampOfLastDelete(), ((CounterColumn)column).timestampOfLastDelete()));
    }

    @Override
    public boolean equals(Object o)
    {
        // super.equals() returns false if o is not a CounterColumn
        return super.equals(o) && timestampOfLastDelete == ((CounterColumn)o).timestampOfLastDelete;
    }

    @Override
    public int hashCode()
    {
        int result = super.hashCode();
        result = 31 * result + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
        return result;
    }

    @Override
    public Column localCopy(ColumnFamilyStore cfs)
    {
        return new CounterColumn(cfs.internOrCopy(name, HeapAllocator.instance), ByteBufferUtil.clone(value), timestamp, timestampOfLastDelete);
    }

    @Override
    public Column localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new CounterColumn(cfs.internOrCopy(name, allocator), allocator.clone(value), timestamp, timestampOfLastDelete);
    }

    @Override
    public String getString(AbstractType<?> comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(false);
        sb.append(":");
        sb.append(contextManager.toString(value));
        sb.append("@");
        sb.append(timestamp());
        sb.append("!");
        sb.append(timestampOfLastDelete);
        return sb.toString();
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    /**
     * Check if a given counterId is found in this CounterColumn context.
     */
    public boolean hasCounterId(CounterId id)
    {
        return contextManager.hasCounterId(value(), id);
    }

    public Column markLocalToBeCleared()
    {
        ByteBuffer marked = contextManager.markLocalToBeCleared(value);
        return marked == value ? this : new CounterColumn(name, marked, timestamp, timestampOfLastDelete);
    }
}
