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
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.RowDataResolver;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

public class SliceFromReadCommand extends ReadCommand
{
    static final Logger logger = LoggerFactory.getLogger(SliceFromReadCommand.class);

    static final SliceFromReadCommandSerializer serializer = new SliceFromReadCommandSerializer();

    public final SliceQueryFilter filter;

    public SliceFromReadCommand(String keyspaceName, ByteBuffer key, String cfName, long timestamp, SliceQueryFilter filter)
    {
        super(keyspaceName, key, cfName, timestamp, Type.GET_SLICES);
        this.filter = filter;
    }

    public ReadCommand copy()
    {
        ReadCommand readCommand = new SliceFromReadCommand(ksName, key, cfName, timestamp, filter);
        readCommand.setDigestQuery(isDigestQuery());
        return readCommand;
    }

    public Row getRow(Keyspace keyspace)
    {
        DecoratedKey dk = StorageService.getPartitioner().decorateKey(key);
        return keyspace.getRow(new QueryFilter(dk, cfName, filter, timestamp));
    }

    @Override
    public ReadCommand maybeGenerateRetryCommand(RowDataResolver resolver, Row row)
    {
        int maxLiveColumns = resolver.getMaxLiveCount();

        int count = filter.count;
        // We generate a retry if at least one node reply with count live columns but after merge we have less
        // than the total number of column we are interested in (which may be < count on a retry).
        // So in particular, if no host returned count live columns, we know it's not a short read.
        if (maxLiveColumns < count)
            return null;

        int liveCountInRow = row == null || row.cf == null ? 0 : filter.getLiveCount(row.cf, timestamp);
        if (liveCountInRow < getOriginalRequestedCount())
        {
            // We asked t (= count) live columns and got l (=liveCountInRow) ones.
            // From that, we can estimate that on this row, for x requested
            // columns, only l/t end up live after reconciliation. So for next
            // round we want to ask x column so that x * (l/t) == t, i.e. x = t^2/l.
            int retryCount = liveCountInRow == 0 ? count + 1 : ((count * count) / liveCountInRow) + 1;
            SliceQueryFilter newFilter = filter.withUpdatedCount(retryCount);
            return new RetriedSliceFromReadCommand(ksName, key, cfName, timestamp, newFilter, getOriginalRequestedCount());
        }

        return null;
    }

    @Override
    public void maybeTrim(Row row)
    {
        if ((row == null) || (row.cf == null))
            return;

        filter.trim(row.cf, getOriginalRequestedCount(), timestamp);
    }

    public IDiskAtomFilter filter()
    {
        return filter;
    }

    public SliceFromReadCommand withUpdatedFilter(SliceQueryFilter newFilter)
    {
        return new SliceFromReadCommand(ksName, key, cfName, timestamp, newFilter);
    }

    /**
     * The original number of columns requested by the user.
     * This can be different from count when the slice command is a retry (see
     * RetriedSliceFromReadCommand)
     */
    protected int getOriginalRequestedCount()
    {
        return filter.count;
    }

    @Override
    public String toString()
    {
        return "SliceFromReadCommand(" +
               "keyspace='" + ksName + '\'' +
               ", key='" + ByteBufferUtil.bytesToHex(key) + '\'' +
               ", cfName='" + cfName + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", filter='" + filter + '\'' +
               ')';
    }
}

class SliceFromReadCommandSerializer implements IVersionedSerializer<ReadCommand>
{
    public void serialize(ReadCommand rm, DataOutput out, int version) throws IOException
    {
        serialize(rm, null, out, version);
    }

    public void serialize(ReadCommand rm, ByteBuffer superColumn, DataOutput out, int version) throws IOException
    {
        SliceFromReadCommand realRM = (SliceFromReadCommand)rm;
        out.writeBoolean(realRM.isDigestQuery());
        out.writeUTF(realRM.ksName);
        ByteBufferUtil.writeWithShortLength(realRM.key, out);

        if (version < MessagingService.VERSION_20)
            new QueryPath(realRM.cfName, superColumn).serialize(out);
        else
            out.writeUTF(realRM.cfName);

        if (version >= MessagingService.VERSION_20)
            out.writeLong(realRM.timestamp);

        SliceQueryFilter.serializer.serialize(realRM.filter, out, version);
    }

    public ReadCommand deserialize(DataInput in, int version) throws IOException
    {
        boolean isDigest = in.readBoolean();
        String keyspaceName = in.readUTF();
        ByteBuffer key = ByteBufferUtil.readWithShortLength(in);

        String cfName;
        ByteBuffer sc = null;
        if (version < MessagingService.VERSION_20)
        {
            QueryPath path = QueryPath.deserialize(in);
            cfName = path.columnFamilyName;
            sc = path.superColumnName;
        }
        else
        {
            cfName = in.readUTF();
        }

        long timestamp = version < MessagingService.VERSION_20 ? System.currentTimeMillis() : in.readLong();

        CFMetaData metadata = Schema.instance.getCFMetaData(keyspaceName, cfName);
        SliceQueryFilter filter;
        if (version < MessagingService.VERSION_20)
        {
            filter = SliceQueryFilter.serializer.deserialize(in, version);

            if (metadata.cfType == ColumnFamilyType.Super)
                filter = SuperColumns.fromSCSliceFilter((CompositeType)metadata.comparator, sc, filter);
        }
        else
        {
            filter = SliceQueryFilter.serializer.deserialize(in, version);
        }

        ReadCommand command = new SliceFromReadCommand(keyspaceName, key, cfName, timestamp, filter);
        command.setDigestQuery(isDigest);
        return command;
    }

    public long serializedSize(ReadCommand cmd, int version)
    {
        return serializedSize(cmd, null, version);
    }

    public long serializedSize(ReadCommand cmd, ByteBuffer superColumn, int version)
    {
        TypeSizes sizes = TypeSizes.NATIVE;
        SliceFromReadCommand command = (SliceFromReadCommand) cmd;
        int keySize = command.key.remaining();

        int size = sizes.sizeof(cmd.isDigestQuery()); // boolean
        size += sizes.sizeof(command.ksName);
        size += sizes.sizeof((short) keySize) + keySize;

        if (version < MessagingService.VERSION_20)
            size += new QueryPath(command.cfName, superColumn).serializedSize(sizes);
        else
            size += sizes.sizeof(command.cfName);

        if (version >= MessagingService.VERSION_20)
            size += sizes.sizeof(cmd.timestamp);

        size += SliceQueryFilter.serializer.serializedSize(command.filter, version);

        return size;
    }
}
