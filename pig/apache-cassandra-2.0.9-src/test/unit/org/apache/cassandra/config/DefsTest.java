/**
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

package org.apache.cassandra.config;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableDeletingTask;
import org.apache.cassandra.locator.OldNetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DefsTest extends SchemaLoader
{
    @Test
    public void testCFMetaDataApply() throws ConfigurationException
    {
        Map<ByteBuffer, ColumnDefinition> indexes = new HashMap<ByteBuffer, ColumnDefinition>();
        for (int i = 0; i < 5; i++)
        {
            ByteBuffer name = ByteBuffer.wrap(new byte[] { (byte)i });
            indexes.put(name, ColumnDefinition.regularDef(name, BytesType.instance, null).setIndex(Integer.toString(i), IndexType.KEYS, null));
        }
        CFMetaData cfm = new CFMetaData("Keyspace1",
                                        "TestApplyCFM_CF",
                                        ColumnFamilyType.Standard,
                                        BytesType.instance);

        cfm.comment("No comment")
           .readRepairChance(0.5)
           .replicateOnWrite(false)
           .gcGraceSeconds(100000)
           .defaultValidator(null)
           .minCompactionThreshold(500)
           .maxCompactionThreshold(500)
           .columnMetadata(indexes);

        // we'll be adding this one later. make sure it's not already there.
        assert cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 5 })) == null;

        CFMetaData cfNew = cfm.clone();

        // add one.
        ColumnDefinition addIndexDef = ColumnDefinition.regularDef(ByteBuffer.wrap(new byte[] { 5 }), BytesType.instance, null)
                                                       .setIndex("5", IndexType.KEYS, null);
        cfNew.addColumnDefinition(addIndexDef);

        // remove one.
        ColumnDefinition removeIndexDef = ColumnDefinition.regularDef(ByteBuffer.wrap(new byte[] { 0 }), BytesType.instance, null)
                                                          .setIndex("0", IndexType.KEYS, null);
        assert cfNew.removeColumnDefinition(removeIndexDef);

        cfm.apply(cfNew);

        for (int i = 1; i < indexes.size(); i++)
            assert cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 1 })) != null;
        assert cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 0 })) == null;
        assert cfm.getColumnDefinition(ByteBuffer.wrap(new byte[] { 5 })) != null;
    }

    @Test
    public void testInvalidNames() throws IOException
    {
        String[] valid = {"1", "a", "_1", "b_", "__", "1_a"};
        for (String s : valid)
            assert CFMetaData.isNameValid(s);

        String[] invalid = {"b@t", "dash-y", "", " ", "dot.s", ".hidden"};
        for (String s : invalid)
            assert !CFMetaData.isNameValid(s);
    }

    @Test
    public void saveAndRestore() throws IOException
    {
        /*
        // verify dump and reload.
        UUID first = UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress());
        DefsTables.dumpToStorage(first);
        List<KSMetaData> defs = new ArrayList<KSMetaData>(DefsTables.loadFromStorage(first));

        assert defs.size() > 0;
        assert defs.size() == Schema.instance.getNonSystemKeyspaces().size();
        for (KSMetaData loaded : defs)
        {
            KSMetaData defined = Schema.instance.getKeyspaceDefinition(loaded.name);
            assert defined.equals(loaded) : String.format("%s != %s", loaded, defined);
        }
        */
    }

    @Test
    public void addNewCfToBogusKeyspace() throws InterruptedException
    {
        CFMetaData newCf = addTestCF("MadeUpKeyspace", "NewCF", "new cf");
        try
        {
            MigrationManager.announceNewColumnFamily(newCf);
            throw new AssertionError("You shouldn't be able to do anything to a keyspace that doesn't exist.");
        }
        catch (ConfigurationException expected)
        {
        }
    }

    @Test
    public void addNewCfWithNullComment() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCfWithNull";
        KSMetaData original = Schema.instance.getKSMetaData(ks);

        CFMetaData newCf = addTestCF(original.name, cf, null);

        assert !Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName);
        MigrationManager.announceNewColumnFamily(newCf);

        assert Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getKSMetaData(ks).cfMetaData().get(newCf.cfName).equals(newCf);
    }

    @Test
    public void addNewCF() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        final String ks = "Keyspace1";
        final String cf = "BrandNewCf";
        KSMetaData original = Schema.instance.getKSMetaData(ks);

        CFMetaData newCf = addTestCF(original.name, cf, "A New Column Family");

        assert !Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName);
        MigrationManager.announceNewColumnFamily(newCf);

        assert Schema.instance.getKSMetaData(ks).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getKSMetaData(ks).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        DecoratedKey dk = Util.dk("key0");
        RowMutation rm = new RowMutation(ks, dk.key);
        rm.add(cf, ByteBufferUtil.bytes("col0"), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Keyspace.open(ks).getColumnFamilyStore(cf);
        assert store != null;
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, cf, FBUtilities.singleton(ByteBufferUtil.bytes("col0"), store.getComparator()), System.currentTimeMillis()));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        Column col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
    }

    @Test
    public void dropCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropCf");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;

        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, ByteBufferUtil.bytes(("col" + i)), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert store.directories.sstableLister().list().size() > 0;

        MigrationManager.announceColumnFamilyDrop(ks.name, cfm.cfName);

        assert !Schema.instance.getKSMetaData(ks.name).cfMetaData().containsKey(cfm.cfName);

        // any write should fail.
        rm = new RowMutation(ks.name, dk.key);
        boolean success = true;
        try
        {
            rm.add("Standard1", ByteBufferUtil.bytes("col0"), ByteBufferUtil.bytes("value0"), 1L);
            rm.apply();
        }
        catch (Throwable th)
        {
            success = false;
        }
        assert !success : "This mutation should have failed since the CF no longer exists.";

        // verify that the files are gone.
        for (File file : store.directories.sstableLister().listFiles())
        {
            if (file.getPath().endsWith("Data.db") && !new File(file.getPath().replace("Data.db", "Compacted")).exists())
                throw new AssertionError("undeleted file " + file);
        }
    }

    @Test
    public void addNewKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("key0");
        CFMetaData newCf = addTestCF("NewKeyspace1", "AddedStandard1", "A new cf for a new ks");

        KSMetaData newKs = KSMetaData.testMetadata(newCf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), newCf);

        MigrationManager.announceNewKeyspace(newKs);

        assert Schema.instance.getKSMetaData(newCf.ksName) != null;
        assert Schema.instance.getKSMetaData(newCf.ksName).equals(newKs);

        // test reads and writes.
        RowMutation rm = new RowMutation(newCf.ksName, dk.key);
        rm.add(newCf.cfName, ByteBufferUtil.bytes("col0"), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Keyspace.open(newCf.ksName).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, newCf.cfName, FBUtilities.singleton(ByteBufferUtil.bytes("col0"), store.getComparator()), System.currentTimeMillis()));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        Column col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
    }

    @Test
    public void dropKS() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData("Keyspace1");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard2");
        assert cfm != null;

        // write some data, force a flush, then verify that files exist on disk.
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, ByteBufferUtil.bytes(("col" + i)), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();
        ColumnFamilyStore store = Keyspace.open(cfm.ksName).getColumnFamilyStore(cfm.cfName);
        assert store != null;
        store.forceBlockingFlush();
        assert store.directories.sstableLister().list().size() > 0;

        MigrationManager.announceKeyspaceDrop(ks.name);

        assert Schema.instance.getKSMetaData(ks.name) == null;

        // write should fail.
        rm = new RowMutation(ks.name, dk.key);
        boolean success = true;
        try
        {
            rm.add("Standard1", ByteBufferUtil.bytes("col0"), ByteBufferUtil.bytes("value0"), 1L);
            rm.apply();
        }
        catch (Throwable th)
        {
            success = false;
        }
        assert !success : "This mutation should have failed since the CF no longer exists.";

        // reads should fail too.
        boolean threw = false;
        try
        {
            Keyspace.open(ks.name);
        }
        catch (Throwable th)
        {
            threw = true;
        }
        assert threw;
    }

    @Test
    public void dropKSUnflushed() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        DecoratedKey dk = Util.dk("dropKs");
        // sanity
        final KSMetaData ks = Schema.instance.getKSMetaData("Keyspace3");
        assert ks != null;
        final CFMetaData cfm = ks.cfMetaData().get("Standard1");
        assert cfm != null;

        // write some data
        RowMutation rm = new RowMutation(ks.name, dk.key);
        for (int i = 0; i < 100; i++)
            rm.add(cfm.cfName, ByteBufferUtil.bytes(("col" + i)), ByteBufferUtil.bytes("anyvalue"), 1L);
        rm.apply();

        MigrationManager.announceKeyspaceDrop(ks.name);

        assert Schema.instance.getKSMetaData(ks.name) == null;
    }

    @Test
    public void createEmptyKsAddNewCf() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        assert Schema.instance.getKSMetaData("EmptyKeyspace") == null;

        KSMetaData newKs = KSMetaData.testMetadata("EmptyKeyspace", SimpleStrategy.class, KSMetaData.optsWithRF(5));

        MigrationManager.announceNewKeyspace(newKs);
        assert Schema.instance.getKSMetaData("EmptyKeyspace") != null;

        CFMetaData newCf = addTestCF("EmptyKeyspace", "AddedLater", "A new CF to add to an empty KS");

        //should not exist until apply
        assert !Schema.instance.getKSMetaData(newKs.name).cfMetaData().containsKey(newCf.cfName);

        //add the new CF to the empty space
        MigrationManager.announceNewColumnFamily(newCf);

        assert Schema.instance.getKSMetaData(newKs.name).cfMetaData().containsKey(newCf.cfName);
        assert Schema.instance.getKSMetaData(newKs.name).cfMetaData().get(newCf.cfName).equals(newCf);

        // now read and write to it.
        DecoratedKey dk = Util.dk("key0");
        RowMutation rm = new RowMutation(newKs.name, dk.key);
        rm.add(newCf.cfName, ByteBufferUtil.bytes("col0"), ByteBufferUtil.bytes("value0"), 1L);
        rm.apply();
        ColumnFamilyStore store = Keyspace.open(newKs.name).getColumnFamilyStore(newCf.cfName);
        assert store != null;
        store.forceBlockingFlush();

        ColumnFamily cfam = store.getColumnFamily(QueryFilter.getNamesFilter(dk, newCf.cfName, FBUtilities.singleton(ByteBufferUtil.bytes("col0"), store.getComparator()), System.currentTimeMillis()));
        assert cfam.getColumn(ByteBufferUtil.bytes("col0")) != null;
        Column col = cfam.getColumn(ByteBufferUtil.bytes("col0"));
        assert ByteBufferUtil.bytes("value0").equals(col.value());
    }

    @Test
    public void testUpdateKeyspace() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        // create a keyspace to serve as existing.
        CFMetaData cf = addTestCF("UpdatedKeyspace", "AddedStandard1", "A new cf for a new ks");
        KSMetaData oldKs = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(5), cf);

        MigrationManager.announceNewKeyspace(oldKs);

        assert Schema.instance.getKSMetaData(cf.ksName) != null;
        assert Schema.instance.getKSMetaData(cf.ksName).equals(oldKs);

        // names should match.
        KSMetaData newBadKs2 = KSMetaData.testMetadata(cf.ksName + "trash", SimpleStrategy.class, KSMetaData.optsWithRF(4));
        try
        {
            MigrationManager.announceKeyspaceUpdate(newBadKs2);
            throw new AssertionError("Should not have been able to update a KS with an invalid KS name.");
        }
        catch (ConfigurationException ex)
        {
            // expected.
        }

        KSMetaData newKs = KSMetaData.testMetadata(cf.ksName, OldNetworkTopologyStrategy.class, KSMetaData.optsWithRF(1));
        MigrationManager.announceKeyspaceUpdate(newKs);

        KSMetaData newFetchedKs = Schema.instance.getKSMetaData(newKs.name);
        assert newFetchedKs.strategyClass.equals(newKs.strategyClass);
        assert !newFetchedKs.strategyClass.equals(oldKs.strategyClass);
    }

    @Test
    public void testUpdateColumnFamilyNoIndexes() throws ConfigurationException, IOException, ExecutionException, InterruptedException
    {
        // create a keyspace with a cf to update.
        CFMetaData cf = addTestCF("UpdatedCfKs", "Standard1added", "A new cf that will be updated");
        KSMetaData ksm = KSMetaData.testMetadata(cf.ksName, SimpleStrategy.class, KSMetaData.optsWithRF(1), cf);
        MigrationManager.announceNewKeyspace(ksm);

        assert Schema.instance.getKSMetaData(cf.ksName) != null;
        assert Schema.instance.getKSMetaData(cf.ksName).equals(ksm);
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName) != null;

        // updating certain fields should fail.
        CFMetaData newCfm = cf.clone();
        newCfm.columnMetadata(new HashMap<ByteBuffer, ColumnDefinition>());
        newCfm.defaultValidator(BytesType.instance);
        newCfm.minCompactionThreshold(5);
        newCfm.maxCompactionThreshold(31);

        // test valid operations.
        newCfm.comment("Modified comment");
        MigrationManager.announceColumnFamilyUpdate(newCfm, false); // doesn't get set back here.

        newCfm.readRepairChance(0.23);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false);

        newCfm.gcGraceSeconds(12);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false);

        newCfm.defaultValidator(UTF8Type.instance);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false);

        newCfm.minCompactionThreshold(3);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false);

        newCfm.maxCompactionThreshold(33);
        MigrationManager.announceColumnFamilyUpdate(newCfm, false);

        // can't test changing the reconciler because there is only one impl.

        // check the cumulative affect.
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getComment().equals(newCfm.getComment());
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getReadRepairChance() == newCfm.getReadRepairChance();
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getGcGraceSeconds() == newCfm.getGcGraceSeconds();
        assert Schema.instance.getCFMetaData(cf.ksName, cf.cfName).getDefaultValidator() == UTF8Type.instance;

        // Change cfId
        newCfm = new CFMetaData(cf.ksName, cf.cfName, cf.cfType, cf.comparator, UUID.randomUUID());
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different id.");
        }
        catch (ConfigurationException expected) {}

        // Change cfName
        newCfm = new CFMetaData(cf.ksName, cf.cfName + "_renamed", cf.cfType, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different name.");
        }
        catch (ConfigurationException expected) {}

        // Change ksName
        newCfm = new CFMetaData(cf.ksName + "_renamed", cf.cfName, cf.cfType, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different keyspace.");
        }
        catch (ConfigurationException expected) {}

        // Change cf type
        newCfm = new CFMetaData(cf.ksName, cf.cfName, ColumnFamilyType.Super, cf.comparator);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blwon up when you used a different cf type.");
        }
        catch (ConfigurationException expected) {}

        // Change comparator
        newCfm = new CFMetaData(cf.ksName, cf.cfName, cf.cfType, TimeUUIDType.instance);
        CFMetaData.copyOpts(newCfm, cf);
        try
        {
            cf.apply(newCfm);
            throw new AssertionError("Should have blown up when you used a different comparator.");
        }
        catch (ConfigurationException expected) {}
    }

    @Test
    public void testDropIndex() throws IOException, ExecutionException, InterruptedException, ConfigurationException
    {
        // persist keyspace definition in the system keyspace
        Schema.instance.getKSMetaData("Keyspace6").toSchema(System.currentTimeMillis()).apply();

        // insert some data.  save the sstable descriptor so we can make sure it's marked for delete after the drop
        RowMutation rm = new RowMutation("Keyspace6", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", ByteBufferUtil.bytes("notbirthdate"), ByteBufferUtil.bytes(1L), 0);
        rm.add("Indexed1", ByteBufferUtil.bytes("birthdate"), ByteBufferUtil.bytes(1L), 0);
        rm.apply();
        ColumnFamilyStore cfs = Keyspace.open("Keyspace6").getColumnFamilyStore("Indexed1");
        cfs.forceBlockingFlush();
        ColumnFamilyStore indexedCfs = cfs.indexManager.getIndexForColumn(ByteBufferUtil.bytes("birthdate")).getIndexCfs();
        Descriptor desc = indexedCfs.getSSTables().iterator().next().descriptor;

        // drop the index
        CFMetaData meta = cfs.metadata.clone();
        ColumnDefinition cdOld = meta.regularColumns().iterator().next();
        ColumnDefinition cdNew = ColumnDefinition.regularDef(cdOld.name, cdOld.getValidator(), null);
        meta.addOrReplaceColumnDefinition(cdNew);
        MigrationManager.announceColumnFamilyUpdate(meta, false);

        // check
        assert cfs.indexManager.getIndexes().isEmpty();
        SSTableDeletingTask.waitForDeletions();
        assert !new File(desc.filenameFor(Component.DATA)).exists();
    }

    private CFMetaData addTestCF(String ks, String cf, String comment)
    {
        CFMetaData newCFMD = new CFMetaData(ks, cf, ColumnFamilyType.Standard, UTF8Type.instance);
        newCFMD.comment(comment)
               .readRepairChance(0.0);

        return newCFMD;
    }
}
