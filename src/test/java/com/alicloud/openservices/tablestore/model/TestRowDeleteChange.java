package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestRowDeleteChange {

    @Test
    public void testConstructorWithInvalidArguments() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowDeleteChange rowChange = new RowDeleteChange("T", primaryKey);
        assertEquals(rowChange.getTableName(), "T");
        assertEquals(rowChange.getPrimaryKey(), primaryKey);

        try {
            new RowDeleteChange("", primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowDeleteChange(null, primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowDeleteChange("T", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowDeleteChange("T", PrimaryKeyBuilder.createPrimaryKeyBuilder().build());
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testCompareTo() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        PrimaryKey primaryKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowDeleteChange rowChange = new RowDeleteChange("T", primaryKey);
        RowDeleteChange rowChange2 = new RowDeleteChange("T", primaryKey2);

        assertTrue(rowChange.compareTo(rowChange2) == 0);
    }

    @Test
    public void testGetDataSize() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(8))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abcde"))
                .build();

        PrimaryKey primaryKey2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(8))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abcde"))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("abcde"))
                .build();

        RowDeleteChange rdc = new RowDeleteChange("TestTable", primaryKey);
        assertEquals(rdc.getDataSize(), 19);

        rdc.setPrimaryKey(primaryKey2);
        assertEquals(rdc.getDataSize(), 27);
    }
}
