/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.context;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.Util;
import org.apache.cassandra.utils.*;

import static org.apache.cassandra.db.context.CounterContext.ContextState;

public class CounterContextTest
{
    private static final CounterContext cc = new CounterContext();

    private static final int headerSizeLength = 2;
    private static final int headerEltLength = 2;
    private static final int idLength = 16;
    private static final int clockLength = 8;
    private static final int countLength = 8;
    private static final int stepLength = idLength + clockLength + countLength;

    /** Allocates 1 byte from a new SlabAllocator and returns it. */
    private Allocator bumpedSlab()
    {
        SlabAllocator allocator = new SlabAllocator();
        allocator.allocate(1);
        return allocator;
    }

    @Test
    public void testAllocate()
    {
        runAllocate(HeapAllocator.instance);
        runAllocate(bumpedSlab());
    }

    private void runAllocate(Allocator allocator)
    {
        ContextState allGlobal = ContextState.allocate(3, 0, 0, allocator);
        assertEquals(headerSizeLength + 3 * headerEltLength + 3 * stepLength, allGlobal.context.remaining());

        ContextState allLocal = ContextState.allocate(0, 3, 0, allocator);
        assertEquals(headerSizeLength + 3 * headerEltLength + 3 * stepLength, allLocal.context.remaining());

        ContextState allRemote = ContextState.allocate(0, 0, 3, allocator);
        assertEquals(headerSizeLength + 3 * stepLength, allRemote.context.remaining());

        ContextState mixed = ContextState.allocate(1, 1, 1, allocator);
        assertEquals(headerSizeLength + 2 * headerEltLength + 3 * stepLength, mixed.context.remaining());
    }

    @Test
    public void testDiff()
    {
        runDiff(HeapAllocator.instance);
        runDiff(bumpedSlab());
    }

    private void runDiff(Allocator allocator)
    {
        ContextState left;
        ContextState right;

        // equality: equal nodes, all counts same
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);
        right = ContextState.wrap(ByteBufferUtil.clone(left.context));

        assertEquals(ContextRelationship.EQUAL, cc.diff(left.context, right.context));

        // greater than: left has superset of nodes (counts equal)
        left = ContextState.allocate(0, 0, 4, allocator);
        left.writeRemote(CounterId.fromInt(3),  3L, 0L);
        left.writeRemote(CounterId.fromInt(6),  2L, 0L);
        left.writeRemote(CounterId.fromInt(9),  1L, 0L);
        left.writeRemote(CounterId.fromInt(12), 0L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 2L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(ContextRelationship.GREATER_THAN, cc.diff(left.context, right.context));

        // less than: left has subset of nodes (counts equal)
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 4, allocator);
        right.writeRemote(CounterId.fromInt(3),  3L, 0L);
        right.writeRemote(CounterId.fromInt(6),  2L, 0L);
        right.writeRemote(CounterId.fromInt(9),  1L, 0L);
        right.writeRemote(CounterId.fromInt(12), 0L, 0L);

        assertEquals(ContextRelationship.LESS_THAN, cc.diff(left.context, right.context));

        // greater than: equal nodes, but left has higher counts
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 2L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(ContextRelationship.GREATER_THAN, cc.diff(left.context, right.context));

        // less than: equal nodes, but right has higher counts
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 3L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 3L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 3L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 3L, 0L);

        assertEquals(ContextRelationship.LESS_THAN, cc.diff(left.context, right.context));

        // disjoint: right and left have disjoint node sets
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(4), 1L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 1L, 0L);
        right.writeRemote(CounterId.fromInt(9), 1L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(4), 1L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(2),  1L, 0L);
        right.writeRemote(CounterId.fromInt(6),  1L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 1L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 1L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 2L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 1L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 1L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: left has more nodes, but lower counts
        left = ContextState.allocate(0, 0, 4, allocator);
        left.writeRemote(CounterId.fromInt(3),  2L, 0L);
        left.writeRemote(CounterId.fromInt(6),  3L, 0L);
        left.writeRemote(CounterId.fromInt(9),  1L, 0L);
        left.writeRemote(CounterId.fromInt(12), 1L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 4L, 0L);
        right.writeRemote(CounterId.fromInt(6), 9L, 0L);
        right.writeRemote(CounterId.fromInt(9), 5L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: left has less nodes, but higher counts
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 3L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 4, allocator);
        right.writeRemote(CounterId.fromInt(3),  4L, 0L);
        right.writeRemote(CounterId.fromInt(6),  3L, 0L);
        right.writeRemote(CounterId.fromInt(9),  2L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        // disjoint: mixed nodes and counts
        left = ContextState.allocate(0, 0, 3, allocator);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 4, allocator);
        right.writeRemote(CounterId.fromInt(3),  4L, 0L);
        right.writeRemote(CounterId.fromInt(6),  3L, 0L);
        right.writeRemote(CounterId.fromInt(9),  2L, 0L);
        right.writeRemote(CounterId.fromInt(12), 1L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));

        left = ContextState.allocate(0, 0, 4, allocator);
        left.writeRemote(CounterId.fromInt(3), 5L, 0L);
        left.writeRemote(CounterId.fromInt(6), 2L, 0L);
        left.writeRemote(CounterId.fromInt(7), 2L, 0L);
        left.writeRemote(CounterId.fromInt(9), 2L, 0L);

        right = ContextState.allocate(0, 0, 3, allocator);
        right.writeRemote(CounterId.fromInt(3), 4L, 0L);
        right.writeRemote(CounterId.fromInt(6), 3L, 0L);
        right.writeRemote(CounterId.fromInt(9), 2L, 0L);

        assertEquals(ContextRelationship.DISJOINT, cc.diff(left.context, right.context));
    }

    @Test
    public void testMerge()
    {
        runMerge(HeapAllocator.instance);
        runMerge(bumpedSlab());
    }

    private void runMerge(Allocator allocator)
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)
        ContextState left = ContextState.allocate(0, 1, 3, allocator);
        left.writeRemote(CounterId.fromInt(1), 1L, 1L);
        left.writeRemote(CounterId.fromInt(2), 2L, 2L);
        left.writeRemote(CounterId.fromInt(4), 6L, 3L);
        left.writeLocal(CounterId.getLocalId(), 7L, 3L);

        ContextState right = ContextState.allocate(0, 1, 2, allocator);
        right.writeRemote(CounterId.fromInt(4), 4L, 4L);
        right.writeRemote(CounterId.fromInt(5), 5L, 5L);
        right.writeLocal(CounterId.getLocalId(), 2L, 9L);

        ByteBuffer merged = cc.merge(left.context, right.context, allocator);
        int hd = 4;

        assertEquals(hd + 5 * stepLength, merged.remaining());
        // local node id's counts are aggregated
        assertTrue(Util.equalsCounterId(CounterId.getLocalId(), merged, hd + 4 * stepLength));
        assertEquals(9L, merged.getLong(merged.position() + hd + 4 * stepLength + idLength));
        assertEquals(12L,  merged.getLong(merged.position() + hd + 4*stepLength + idLength + clockLength));

        // remote node id counts are reconciled (i.e. take max)
        assertTrue(Util.equalsCounterId(CounterId.fromInt(4), merged, hd + 2 * stepLength));
        assertEquals(6L, merged.getLong(merged.position() + hd + 2 * stepLength + idLength));
        assertEquals( 3L,  merged.getLong(merged.position() + hd + 2*stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(5), merged, hd + 3 * stepLength));
        assertEquals(5L, merged.getLong(merged.position() + hd + 3 * stepLength + idLength));
        assertEquals( 5L,  merged.getLong(merged.position() + hd + 3*stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, hd + stepLength));
        assertEquals(2L, merged.getLong(merged.position() + hd + stepLength + idLength));
        assertEquals( 2L,  merged.getLong(merged.position() + hd + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, hd));
        assertEquals( 1L,  merged.getLong(merged.position() + hd + idLength));
        assertEquals( 1L,  merged.getLong(merged.position() + hd + idLength + clockLength));

        //
        // Test merging two exclusively global contexts
        //
        left = ContextState.allocate(3, 0, 0, allocator);
        left.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        left.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        left.writeGlobal(CounterId.fromInt(3), 3L, 3L);

        right = ContextState.allocate(3, 0, 0, allocator);
        right.writeGlobal(CounterId.fromInt(3), 6L, 6L);
        right.writeGlobal(CounterId.fromInt(4), 4L, 4L);
        right.writeGlobal(CounterId.fromInt(5), 5L, 5L);

        merged = cc.merge(left.context, right.context, allocator);
        assertEquals(headerSizeLength + 5 * headerEltLength + 5 * stepLength, merged.remaining());
        assertEquals(18L, cc.total(merged));
        assertEquals(5, merged.getShort(merged.position()));

        int headerLength = headerSizeLength + 5 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, headerLength + stepLength));
        assertEquals(2L, merged.getLong(merged.position() + headerLength + stepLength + idLength));
        assertEquals(2L, merged.getLong(merged.position() + headerLength + stepLength + idLength + clockLength));
        // pick the global shard with the largest clock
        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), merged, headerLength + 2 * stepLength));
        assertEquals(6L, merged.getLong(merged.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(6L, merged.getLong(merged.position() + headerLength + 2 * stepLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(4), merged, headerLength + 3 * stepLength));
        assertEquals(4L, merged.getLong(merged.position() + headerLength + 3 * stepLength + idLength));
        assertEquals(4L, merged.getLong(merged.position() + headerLength + 3 * stepLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(5), merged, headerLength + 4 * stepLength));
        assertEquals(5L, merged.getLong(merged.position() + headerLength + 4 * stepLength + idLength));
        assertEquals(5L, merged.getLong(merged.position() + headerLength + 4 * stepLength + idLength + clockLength));

        //
        // Test merging two global contexts w/ 'invalid shards'
        //
        left = ContextState.allocate(1, 0, 0, allocator);
        left.writeGlobal(CounterId.fromInt(1), 10L, 20L);

        right = ContextState.allocate(1, 0, 0, allocator);
        right.writeGlobal(CounterId.fromInt(1), 10L, 30L);

        merged = cc.merge(left.context, right.context, allocator);
        headerLength = headerSizeLength + headerEltLength;
        assertEquals(headerLength + stepLength, merged.remaining());
        assertEquals(30L, cc.total(merged));
        assertEquals(1, merged.getShort(merged.position()));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(10L, merged.getLong(merged.position() + headerLength + idLength));
        // with equal clock, we should pick the largest value
        assertEquals(30L, merged.getLong(merged.position() + headerLength + idLength + clockLength));

        //
        // Test merging global w/ mixed contexts
        //
        left = ContextState.allocate(2, 0, 0, allocator);
        left.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        left.writeGlobal(CounterId.fromInt(2), 1L, 1L);

        right = ContextState.allocate(0, 1, 1, allocator);
        right.writeLocal(CounterId.fromInt(1), 100L, 100L);
        right.writeRemote(CounterId.fromInt(2), 100L, 100L);

        // global shards should dominate local/remote, even with lower clock and value
        merged = cc.merge(left.context, right.context, allocator);
        headerLength = headerSizeLength + 2 * headerEltLength;
        assertEquals(headerLength + 2 * stepLength, merged.remaining());
        assertEquals(2L, cc.total(merged));
        assertEquals(2, merged.getShort(merged.position()));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), merged, headerLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + idLength + clockLength));
        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), merged, headerLength + stepLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + stepLength + idLength));
        assertEquals(1L, merged.getLong(merged.position() + headerLength + stepLength + idLength + clockLength));
    }

    @Test
    public void testTotal()
    {
        runTotal(HeapAllocator.instance);
        runTotal(bumpedSlab());
    }

    private void runTotal(Allocator allocator)
    {
        ContextState mixed = ContextState.allocate(0, 1, 4, allocator);
        mixed.writeRemote(CounterId.fromInt(1), 1L, 1L);
        mixed.writeRemote(CounterId.fromInt(2), 2L, 2L);
        mixed.writeRemote(CounterId.fromInt(4), 4L, 4L);
        mixed.writeRemote(CounterId.fromInt(5), 5L, 5L);
        mixed.writeLocal(CounterId.getLocalId(), 12L, 12L);
        assertEquals(24L, cc.total(mixed.context));

        ContextState global = ContextState.allocate(3, 0, 0, allocator);
        global.writeGlobal(CounterId.fromInt(1), 1L, 1L);
        global.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        global.writeGlobal(CounterId.fromInt(3), 3L, 3L);
        assertEquals(6L, cc.total(global.context));
    }

    @Test
    public void testClearLocal()
    {
        ContextState state;
        ByteBuffer marked;
        ByteBuffer cleared;
        Allocator allocator = HeapAllocator.instance;

        // mark/clear for remote-only contexts is a no-op
        state = ContextState.allocate(0, 0, 1, allocator);
        state.writeRemote(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertEquals(0, marked.getShort(marked.position()));
        assertSame(state.context, marked); // should return the original context

        cleared = cc.clearAllLocal(marked);
        assertSame(cleared, marked); // shouldn't alter anything either

        // a single local shard
        state = ContextState.allocate(0, 1, 0, allocator);
        state.writeLocal(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertTrue(cc.shouldClearLocal(marked));
        assertEquals(-1, marked.getShort(marked.position()));
        assertNotSame(state.context, marked); // shouldn't alter in place, as it used to do

        cleared = cc.clearAllLocal(marked);
        assertFalse(cc.shouldClearLocal(cleared));
        assertEquals(0, cleared.getShort(cleared.position()));

        // 2 global + 1 local shard
        state = ContextState.allocate(2, 1, 0, allocator);
        state.writeLocal(CounterId.fromInt(1), 1L, 1L);
        state.writeGlobal(CounterId.fromInt(2), 2L, 2L);
        state.writeGlobal(CounterId.fromInt(3), 3L, 3L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertTrue(cc.shouldClearLocal(marked));

        assertEquals(-3, marked.getShort(marked.position()));
        assertEquals(0, marked.getShort(marked.position() + headerSizeLength));
        assertEquals(Short.MIN_VALUE + 1, marked.getShort(marked.position() + headerSizeLength + headerEltLength));
        assertEquals(Short.MIN_VALUE + 2, marked.getShort(marked.position() + headerSizeLength + 2 * headerEltLength));

        int headerLength = headerSizeLength + 3 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), marked, headerLength));
        assertEquals(1L, marked.getLong(marked.position() + headerLength + idLength));
        assertEquals(1L, marked.getLong(marked.position() + headerLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), marked, headerLength + stepLength));
        assertEquals(2L, marked.getLong(marked.position() + headerLength + stepLength + idLength));
        assertEquals(2L, marked.getLong(marked.position() + headerLength + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), marked, headerLength + 2 * stepLength));
        assertEquals(3L, marked.getLong(marked.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(3L, marked.getLong(marked.position() + headerLength + 2 * stepLength + idLength + clockLength));

        cleared = cc.clearAllLocal(marked);
        assertFalse(cc.shouldClearLocal(cleared));

        assertEquals(2, cleared.getShort(cleared.position())); // 2 global shards
        assertEquals(Short.MIN_VALUE + 1, cleared.getShort(marked.position() + headerEltLength));
        assertEquals(Short.MIN_VALUE + 2, cleared.getShort(marked.position() + headerSizeLength + headerEltLength));

        headerLength = headerSizeLength + 2 * headerEltLength;
        assertTrue(Util.equalsCounterId(CounterId.fromInt(1), cleared, headerLength));
        assertEquals(1L, cleared.getLong(cleared.position() + headerLength + idLength));
        assertEquals(1L, cleared.getLong(cleared.position() + headerLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(2), cleared, headerLength + stepLength));
        assertEquals(2L, cleared.getLong(cleared.position() + headerLength + stepLength + idLength));
        assertEquals(2L, cleared.getLong(cleared.position() + headerLength + stepLength + idLength + clockLength));

        assertTrue(Util.equalsCounterId(CounterId.fromInt(3), cleared, headerLength + 2 * stepLength));
        assertEquals(3L, cleared.getLong(cleared.position() + headerLength + 2 * stepLength + idLength));
        assertEquals(3L, cleared.getLong(cleared.position() + headerLength + 2 * stepLength + idLength + clockLength));

        // a single global shard - no-op
        state = ContextState.allocate(1, 0, 0, allocator);
        state.writeGlobal(CounterId.fromInt(1), 1L, 1L);

        assertFalse(cc.shouldClearLocal(state.context));
        marked = cc.markLocalToBeCleared(state.context);
        assertEquals(1, marked.getShort(marked.position()));
        assertSame(state.context, marked);

        cleared = cc.clearAllLocal(marked);
        assertSame(cleared, marked);
    }
}
