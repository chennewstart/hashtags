/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.Util.addMutation;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.junit.Test;

public class NameSortTest extends SchemaLoader
{
    @Test
    public void testNameSort1() throws IOException
    {
        // single key
        testNameSort(1);
    }

    @Test
    public void testNameSort10() throws IOException
    {
        // multiple keys, flushing concurrently w/ inserts
        testNameSort(10);
    }

    @Test
    public void testNameSort100() throws IOException
    {
        // enough keys to force compaction concurrently w/ inserts
        testNameSort(100);
    }

    private void testNameSort(int N) throws IOException
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");

        for (int i = 0; i < N; ++i)
        {
            ByteBuffer key = ByteBufferUtil.bytes(Integer.toString(i));
            RowMutation rm;

            // standard
            for (int j = 0; j < 8; ++j)
            {
                ByteBuffer bytes = j % 2 == 0 ? ByteBufferUtil.bytes("a") : ByteBufferUtil.bytes("b");
                rm = new RowMutation("Keyspace1", key);
                rm.add("Standard1", ByteBufferUtil.bytes(("Column-" + j)), bytes, j);
                rm.applyUnsafe();
            }

            // super
            for (int j = 0; j < 8; ++j)
            {
                rm = new RowMutation("Keyspace1", key);
                for (int k = 0; k < 4; ++k)
                {
                    String value = (j + k) % 2 == 0 ? "a" : "b";
                    addMutation(rm, "Super1", "SuperColumn-" + j, k, value, k);
                }
                rm.applyUnsafe();
            }
        }

        validateNameSort(keyspace, N);

        keyspace.getColumnFamilyStore("Standard1").forceBlockingFlush();
        keyspace.getColumnFamilyStore("Super1").forceBlockingFlush();
        validateNameSort(keyspace, N);
    }

    private void validateNameSort(Keyspace keyspace, int N) throws IOException
    {
        for (int i = 0; i < N; ++i)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            ColumnFamily cf;

            cf = Util.getColumnFamily(keyspace, key, "Standard1");
            Collection<Column> columns = cf.getSortedColumns();
            for (Column column : columns)
            {
                String name = ByteBufferUtil.string(column.name());
                int j = Integer.valueOf(name.substring(name.length() - 1));
                byte[] bytes = j % 2 == 0 ? "a".getBytes() : "b".getBytes();
                assertEquals(new String(bytes), ByteBufferUtil.string(column.value()));
            }
        }
    }
}
