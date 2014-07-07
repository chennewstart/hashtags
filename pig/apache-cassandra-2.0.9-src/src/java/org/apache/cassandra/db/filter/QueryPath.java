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
package org.apache.cassandra.db.filter;

import java.io.*;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * This class is obsolete internally, but kept for wire compatibility with
 * older nodes. I.e. we kept it only for the serialization part.
 */
public class QueryPath
{
    public final String columnFamilyName;
    public final ByteBuffer superColumnName;
    public final ByteBuffer columnName;

    public QueryPath(String columnFamilyName, ByteBuffer superColumnName, ByteBuffer columnName)
    {
        this.columnFamilyName = columnFamilyName;
        this.superColumnName = superColumnName;
        this.columnName = columnName;
    }

    public QueryPath(String columnFamilyName, ByteBuffer superColumnName)
    {
        this(columnFamilyName, superColumnName, null);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "columnFamilyName='" + columnFamilyName + '\'' +
               ", superColumnName='" + superColumnName + '\'' +
               ", columnName='" + columnName + '\'' +
               ')';
    }

    public void serialize(DataOutput out) throws IOException
    {
        assert !"".equals(columnFamilyName);
        assert superColumnName == null || superColumnName.remaining() > 0;
        assert columnName == null || columnName.remaining() > 0;
        out.writeUTF(columnFamilyName == null ? "" : columnFamilyName);
        ByteBufferUtil.writeWithShortLength(superColumnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : superColumnName, out);
        ByteBufferUtil.writeWithShortLength(columnName == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnName, out);
    }

    public static QueryPath deserialize(DataInput din) throws IOException
    {
        String cfName = din.readUTF();
        ByteBuffer scName = ByteBufferUtil.readWithShortLength(din);
        ByteBuffer cName = ByteBufferUtil.readWithShortLength(din);
        return new QueryPath(cfName.isEmpty() ? null : cfName,
                             scName.remaining() == 0 ? null : scName,
                             cName.remaining() == 0 ? null : cName);
    }

    public int serializedSize(TypeSizes typeSizes)
    {
        int size = 0;

        if (columnFamilyName == null)
            size += typeSizes.sizeof((short) 0);
        else
            size += typeSizes.sizeof(columnFamilyName);

        if (superColumnName == null)
        {
            size += typeSizes.sizeof((short) 0);
        }
        else
        {
            int scNameSize = superColumnName.remaining();
            size += typeSizes.sizeof((short) scNameSize);
            size += scNameSize;
        }

        if (columnName == null)
        {
            size += typeSizes.sizeof((short) 0);
        }
        else
        {
            int cNameSize = columnName.remaining();
            size += typeSizes.sizeof((short) cNameSize);
            size += cNameSize;
        }

        return size;
    }
}
