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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class ColumnFamilySerializer implements IVersionedSerializer<ColumnFamily>, ISSTableSerializer<ColumnFamily>
{
    /*
     * Serialized ColumnFamily format:
     *
     * [serialized for intra-node writes only, e.g. returning a query result]
     * <cf nullability boolean: false if the cf is null>
     * <cf id>
     *
     * [in sstable only]
     * <column bloom filter>
     * <sparse column index, start/finish columns every ColumnIndexSizeInKB of data>
     *
     * [always present]
     * <local deletion time>
     * <client-provided deletion time>
     * <column count>
     * <columns, serialized individually>
    */
    public void serialize(ColumnFamily cf, DataOutput out, int version)
    {
        try
        {
            if (cf == null)
            {
                out.writeBoolean(false);
                return;
            }

            out.writeBoolean(true);
            serializeCfId(cf.id(), out, version);

            if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
            {
                SuperColumns.serializeSuperColumnFamily(cf, out, version);
                return;
            }

            DeletionInfo.serializer().serialize(cf.deletionInfo(), out, version);
            ColumnSerializer columnSerializer = Column.serializer;
            int count = cf.getColumnCount();
            out.writeInt(count);
            int written = 0;
            for (Column column : cf)
            {
                columnSerializer.serialize(column, out);
                written++;
            }
            assert count == written: "Column family had " + count + " columns, but " + written + " written";
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ColumnFamily deserialize(DataInput in, int version) throws IOException
    {
        return deserialize(in, ColumnSerializer.Flag.LOCAL, version);
    }

    public ColumnFamily deserialize(DataInput in, ColumnSerializer.Flag flag, int version) throws IOException
    {
        return deserialize(in, ArrayBackedSortedColumns.factory, flag, version);
    }

    public ColumnFamily deserialize(DataInput in, ColumnFamily.Factory factory, ColumnSerializer.Flag flag, int version) throws IOException
    {
        if (!in.readBoolean())
            return null;

        ColumnFamily cf = factory.create(Schema.instance.getCFMetaData(deserializeCfId(in, version)));

        if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
        {
            SuperColumns.deserializerSuperColumnFamily(in, cf, flag, version);
        }
        else
        {
            cf.delete(DeletionInfo.serializer().deserialize(in, version, cf.getComparator()));

            ColumnSerializer columnSerializer = Column.serializer;
            int size = in.readInt();
            for (int i = 0; i < size; ++i)
                cf.addColumn(columnSerializer.deserialize(in, flag));
        }
        return cf;
    }

    public long contentSerializedSize(ColumnFamily cf, TypeSizes typeSizes, int version)
    {
        long size = 0L;

        if (cf.metadata().isSuper() && version < MessagingService.VERSION_20)
        {
            size += SuperColumns.serializedSize(cf, typeSizes, version);
        }
        else
        {
            size += DeletionInfo.serializer().serializedSize(cf.deletionInfo(), typeSizes, version);
            size += typeSizes.sizeof(cf.getColumnCount());
            for (Column column : cf)
                size += column.serializedSize(typeSizes);
        }
        return size;
    }

    public long serializedSize(ColumnFamily cf, TypeSizes typeSizes, int version)
    {
        if (cf == null)
        {
            return typeSizes.sizeof(false);
        }
        else
        {
            return typeSizes.sizeof(true)  /* nullness bool */
                 + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                 + contentSerializedSize(cf, typeSizes, version);
        }
    }

    public long serializedSize(ColumnFamily cf, int version)
    {
        return serializedSize(cf, TypeSizes.NATIVE, version);
    }

    public void serializeForSSTable(ColumnFamily cf, DataOutput out)
    {
        // Column families shouldn't be written directly to disk, use ColumnIndex.Builder instead
        throw new UnsupportedOperationException();
    }

    public ColumnFamily deserializeFromSSTable(DataInput in, Descriptor.Version version)
    {
        throw new UnsupportedOperationException();
    }

    public void serializeCfId(UUID cfId, DataOutput out, int version) throws IOException
    {
        UUIDSerializer.serializer.serialize(cfId, out, version);
    }

    public UUID deserializeCfId(DataInput in, int version) throws IOException
    {
        UUID cfId = UUIDSerializer.serializer.deserialize(in, version);
        if (Schema.instance.getCF(cfId) == null)
            throw new UnknownColumnFamilyException("Couldn't find cfId=" + cfId, cfId);

        return cfId;
    }

    public int cfIdSerializedSize(UUID cfId, TypeSizes typeSizes, int version)
    {
        return typeSizes.sizeof(cfId);
    }
}
