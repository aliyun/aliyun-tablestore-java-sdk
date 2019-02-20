package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPrimaryKey {

    @Test
    public void testOperations() {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        PrimaryKey primaryKey = builder.build();

        PrimaryKeyColumn[] pks = primaryKey.getPrimaryKeyColumns();
        assertEquals(primaryKey.size(), 5);
        for (int i = 0; i < primaryKey.size(); i++) {
            assertEquals(primaryKey.getPrimaryKeyColumn(i), pks[i]);
        }

        assertEquals(primaryKey.getPrimaryKeyColumn("A").getValue(), PrimaryKeyValue.fromLong(1));
        assertEquals(primaryKey.getPrimaryKeyColumn("B").getValue(), PrimaryKeyValue.fromLong(2));
        assertEquals(primaryKey.getPrimaryKeyColumn("C").getValue(), PrimaryKeyValue.fromLong(3));
        assertEquals(primaryKey.getPrimaryKeyColumn("D").getValue(), PrimaryKeyValue.fromLong(4));
        assertEquals(primaryKey.getPrimaryKeyColumn("E").getValue(), PrimaryKeyValue.fromLong(5));

        assertTrue(primaryKey.contains("A"));
        assertTrue(primaryKey.contains("B"));
        assertTrue(primaryKey.contains("C"));
        assertTrue(primaryKey.contains("D"));
        assertTrue(primaryKey.contains("E"));
        assertTrue(!primaryKey.contains("a"));

        try {
            primaryKey.getPrimaryKeyColumn(5);
            fail();
        } catch (IllegalArgumentException e) {

        }

        assertTrue(!primaryKey.isEmpty());
    }

    @Test
    public void testEquals() {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        PrimaryKey primaryKey1 = builder.build();
        PrimaryKey primaryKey2 = builder.build();

        assertEquals(primaryKey1, primaryKey2);
        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());

        PrimaryKeyColumn[] tmp = primaryKey1.getPrimaryKeyColumns();
        tmp[2] = new PrimaryKeyColumn("C", PrimaryKeyValue.fromLong(4));

        PrimaryKey primaryKey3 = new PrimaryKey(tmp);
        assertTrue(!primaryKey1.equals(primaryKey3));

        PrimaryKey primaryKey4 = new PrimaryKey(Arrays.copyOf(primaryKey1.getPrimaryKeyColumns(), 4));
        assertTrue(!primaryKey1.equals(primaryKey4));
    }

    @Test
    public void testCompareTo() {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        PrimaryKey primaryKey1 = builder.build();
        PrimaryKey primaryKey2 = builder.build();

        assertTrue(primaryKey1.compareTo(primaryKey2) == 0);
        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());

        PrimaryKeyColumn[] tmp = primaryKey1.getPrimaryKeyColumns();
        tmp[2] = new PrimaryKeyColumn("C", PrimaryKeyValue.fromLong(4));

        PrimaryKey primaryKey3 = new PrimaryKey(tmp);
        assertTrue(primaryKey1.compareTo(primaryKey3) < 0);
        assertTrue(primaryKey3.compareTo(primaryKey1) > 0);
    }

    @Test
    public void testCompareTo_InvalidArguments() {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));
        PrimaryKey primaryKey1 = builder.build();

        // compare two primary key with different length
        PrimaryKey primaryKey2 = new PrimaryKey(Arrays.copyOf(primaryKey1.getPrimaryKeyColumns(), 4));
        try {
            primaryKey1.compareTo(primaryKey2);
            fail();
        } catch (IllegalArgumentException e) {

        }

        // compare two primary key with different name
        PrimaryKeyColumn[] tmp = primaryKey1.getPrimaryKeyColumns();
        tmp[2] = new PrimaryKeyColumn("CC", PrimaryKeyValue.fromLong(3));

        PrimaryKey primaryKey3 = new PrimaryKey(tmp);
        try {
            primaryKey1.compareTo(primaryKey3);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new PrimaryKey(new PrimaryKeyColumn[0]);
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testGetDataSize() {
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(8))
                    .build();
            assertEquals(primaryKey.getDataSize(), 11);
        }
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .build();
            assertEquals(primaryKey.getDataSize(), 6);
        }
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromString("abc"))
                    .build();
            assertEquals(primaryKey.getDataSize(), 6);
        }
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.AUTO_INCREMENT)
                    .build();
            assertEquals(primaryKey.getDataSize(), 3);
        }
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MIN)
                    .build();
            assertEquals(primaryKey.getDataSize(), 3);
        }
        {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MAX)
                    .build();
            assertEquals(primaryKey.getDataSize(), 3);
        }
    }
}
