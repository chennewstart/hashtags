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

import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.db.filter.QueryFilter;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;


public class RemoveColumnTest extends SchemaLoader
{
    @Test
    public void testRemoveColumn()
    {
        Keyspace keyspace = Keyspace.open("Keyspace1");
        ColumnFamilyStore store = keyspace.getColumnFamilyStore("Standard1");
        RowMutation rm;
        DecoratedKey dk = Util.dk("key1");

        // add data
        rm = new RowMutation("Keyspace1", dk.key);
        rm.add("Standard1", ByteBufferUtil.bytes("Column1"), ByteBufferUtil.bytes("asdf"), 0);
        rm.apply();
        store.forceBlockingFlush();

        // remove
        rm = new RowMutation("Keyspace1", dk.key);
        rm.delete("Standard1", ByteBufferUtil.bytes("Column1"), 1);
        rm.apply();

        ColumnFamily retrieved = store.getColumnFamily(QueryFilter.getNamesFilter(dk,
                                                                                  "Standard1",
                                                                                  FBUtilities.singleton(ByteBufferUtil.bytes("Column1"), store.getComparator()),
                                                                                  System.currentTimeMillis()));
        assert retrieved.getColumn(ByteBufferUtil.bytes("Column1")).isMarkedForDelete(System.currentTimeMillis());
        assertNull(Util.cloneAndRemoveDeleted(retrieved, Integer.MAX_VALUE));
        assertNull(Util.cloneAndRemoveDeleted(store.getColumnFamily(QueryFilter.getIdentityFilter(dk,
                                                                                                  "Standard1",
                                                                                                  System.currentTimeMillis())),
                                              Integer.MAX_VALUE));
    }

    @Test
    public void deletedColumnShouldAlwaysBeMarkedForDelete()
    {
        // Check for bug in #4307
        long timestamp = System.currentTimeMillis();
        int localDeletionTime = (int) (timestamp / 1000);
        Column c = DeletedColumn.create(localDeletionTime, timestamp, "dc1");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete(timestamp));

        // Simulate a node that is 30 seconds behind
        c = DeletedColumn.create(localDeletionTime + 30, timestamp + 30000, "dc2");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete(timestamp));

        // Simulate a node that is 30 ahead behind
        c = DeletedColumn.create(localDeletionTime - 30, timestamp - 30000, "dc3");
        assertTrue("DeletedColumn was not marked for delete", c.isMarkedForDelete(timestamp));
    }

}
