package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTableMeta {

    @Test
    public void testOperations() {
        TableMeta tableMeta = new TableMeta("T");
        assertEquals(tableMeta.getTableName(), "T");
        assertTrue(tableMeta.getPrimaryKeyList().isEmpty());
        assertTrue(tableMeta.getPrimaryKeyMap().isEmpty());

        tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("PK2", PrimaryKeyType.BINARY);

        assertEquals(tableMeta.getPrimaryKeyList().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER));
        assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        assertEquals(tableMeta.getPrimaryKeyList().get(2), new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY));

        assertEquals(tableMeta.getPrimaryKeyMap().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK0"), PrimaryKeyType.INTEGER);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK1"), PrimaryKeyType.STRING);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK2"), PrimaryKeyType.BINARY);
    }

    @Test
    public void testAddPKs_WithArray() {
        TableMeta tableMeta = new TableMeta("T");
        PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
        pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER);
        pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.STRING);
        pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY);

        tableMeta.addPrimaryKeyColumns(pks);
        assertEquals(tableMeta.getPrimaryKeyList().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER));
        assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        assertEquals(tableMeta.getPrimaryKeyList().get(2), new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY));

        assertEquals(tableMeta.getPrimaryKeyMap().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK0"), PrimaryKeyType.INTEGER);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK1"), PrimaryKeyType.STRING);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK2"), PrimaryKeyType.BINARY);
    }

    @Test
    public void testAddPKs_WithList() {
        TableMeta tableMeta = new TableMeta("T");
        PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
        pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER);
        pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.STRING);
        pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY);

        tableMeta.addPrimaryKeyColumns(Arrays.asList(pks));
        assertEquals(tableMeta.getPrimaryKeyList().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER));
        assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        assertEquals(tableMeta.getPrimaryKeyList().get(2), new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY));

        assertEquals(tableMeta.getPrimaryKeyMap().size(), 3);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK0"), PrimaryKeyType.INTEGER);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK1"), PrimaryKeyType.STRING);
        assertEquals(tableMeta.getPrimaryKeyMap().get("PK2"), PrimaryKeyType.BINARY);
    }

    @Test
    public void testPkAutoIncrement() {
        {
            TableMeta tableMeta = new TableMeta("T");
            PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
            pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.STRING);
            pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY);

            tableMeta.addPrimaryKeyColumns(Arrays.asList(pks));
            assertEquals(tableMeta.getPrimaryKeyList().size(), 3);
            assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.STRING));
            assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(tableMeta.getPrimaryKeyList().get(2), new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY));

            assertEquals(tableMeta.getPrimaryKeyMap().size(), 3);
            assertEquals(tableMeta.getPrimaryKeyMap().get("PK0"), PrimaryKeyType.STRING);
            assertEquals(tableMeta.getPrimaryKeyMap().get("PK1"), PrimaryKeyType.INTEGER);
            assertEquals(tableMeta.getPrimaryKeyMap().get("PK2"), PrimaryKeyType.BINARY);
        }

        {
            TableMeta tableMeta = new TableMeta("T");

            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            assertEquals(tableMeta.getPrimaryKeyList().size(), 2);
            assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.STRING));
            assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
        }

        {
            TableMeta tableMeta = new TableMeta("T");

            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.STRING);
            tableMeta.addAutoIncrementPrimaryKeyColumn("PK1");

            assertEquals(tableMeta.getPrimaryKeyList().size(), 2);
            assertEquals(tableMeta.getPrimaryKeyList().get(0), new PrimaryKeySchema("PK0", PrimaryKeyType.STRING));
            assertEquals(tableMeta.getPrimaryKeyList().get(1), new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
        }
    }

    @Test
    public void testJsonizer() {
        TableMeta tableMeta = new TableMeta("T");

        tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.STRING);
        tableMeta.addAutoIncrementPrimaryKeyColumn("PK1");

        String json = tableMeta.jsonize();
        assertEquals("{\n" +
                "  \"TableName\": \"T\",\n" +
                "  \"PrimaryKey\": [\n" +
                "    {\"Name\": \"PK0\", \"Type\": \"STRING\"},\n" +
                "    {\"Name\": \"PK1\", \"Type\": \"INTEGER\", \"Option\":\"AUTO_INCREMENT\"}],\n  \"DefinedColumn\": [\n" +
                "    ]}", json);
    }

    @Test
    public void testInvalidArguments() {
        try {
            new TableMeta("");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new TableMeta(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            tableMeta.setTableName(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            tableMeta.setTableName("");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
            pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.STRING);
            pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY);

            tableMeta.addPrimaryKeyColumns(Arrays.asList(pks));
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
            pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER);
            pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.STRING, PrimaryKeyOption.AUTO_INCREMENT);
            pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY);

            tableMeta.addPrimaryKeyColumns(Arrays.asList(pks));
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            PrimaryKeySchema[] pks = new PrimaryKeySchema[3];
            pks[0] = new PrimaryKeySchema("PK0", PrimaryKeyType.INTEGER);
            pks[1] = new PrimaryKeySchema("PK1", PrimaryKeyType.STRING);
            pks[2] = new PrimaryKeySchema("PK2", PrimaryKeyType.BINARY, PrimaryKeyOption.AUTO_INCREMENT);

            tableMeta.addPrimaryKeyColumns(Arrays.asList(pks));
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.STRING, PrimaryKeyOption.AUTO_INCREMENT);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            TableMeta tableMeta = new TableMeta("T");
            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.BINARY, PrimaryKeyOption.AUTO_INCREMENT);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testGetPrimaryKeyMap() {
        TableMeta tableMeta = new TableMeta("TestTable");
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);

        Map<String, PrimaryKeyType> primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
        Map<String, PrimaryKeySchema> primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

        assertEquals(primaryKeySchemaMap.size(), 2);
        assertEquals(primaryKeyTypeMap.size(), 2);

        assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
        assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
        assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
        assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);

        {
            tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 3);
            assertEquals(primaryKeyTypeMap.size(), 3);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
        }

        {
            tableMeta.addAutoIncrementPrimaryKeyColumn("pk3");
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 4);
            assertEquals(primaryKeyTypeMap.size(), 4);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk3"), new PrimaryKeySchema("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk3"), PrimaryKeyType.INTEGER);
        }

        {
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk4", PrimaryKeyType.BINARY));
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 5);
            assertEquals(primaryKeyTypeMap.size(), 5);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk3"), new PrimaryKeySchema("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk4"), new PrimaryKeySchema("pk4", PrimaryKeyType.BINARY));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk3"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk4"), PrimaryKeyType.BINARY);
        }

        {
            tableMeta.addPrimaryKeyColumn("pk5", PrimaryKeyType.INTEGER);
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 6);
            assertEquals(primaryKeyTypeMap.size(), 6);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk3"), new PrimaryKeySchema("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk4"), new PrimaryKeySchema("pk4", PrimaryKeyType.BINARY));
            assertEquals(primaryKeySchemaMap.get("pk5"), new PrimaryKeySchema("pk5", PrimaryKeyType.INTEGER));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk3"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk4"), PrimaryKeyType.BINARY);
            assertEquals(primaryKeyTypeMap.get("pk5"), PrimaryKeyType.INTEGER);
        }

        {
            tableMeta.addPrimaryKeyColumns(Arrays.asList(new PrimaryKeySchema("pk6", PrimaryKeyType.STRING)));
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 7);
            assertEquals(primaryKeyTypeMap.size(), 7);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk3"), new PrimaryKeySchema("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk4"), new PrimaryKeySchema("pk4", PrimaryKeyType.BINARY));
            assertEquals(primaryKeySchemaMap.get("pk5"), new PrimaryKeySchema("pk5", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk6"), new PrimaryKeySchema("pk6", PrimaryKeyType.STRING));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk3"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk4"), PrimaryKeyType.BINARY);
            assertEquals(primaryKeyTypeMap.get("pk5"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk6"), PrimaryKeyType.STRING);
        }

        {
            tableMeta.addPrimaryKeyColumns(new PrimaryKeySchema[]{new PrimaryKeySchema("pk7", PrimaryKeyType.STRING)});
            primaryKeyTypeMap = tableMeta.getPrimaryKeyMap();
            primaryKeySchemaMap = tableMeta.getPrimaryKeySchemaMap();

            assertEquals(primaryKeySchemaMap.size(), 8);
            assertEquals(primaryKeyTypeMap.size(), 8);

            assertEquals(primaryKeySchemaMap.get("pk0"), new PrimaryKeySchema("pk0", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk1"), new PrimaryKeySchema("pk1", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk2"), new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk3"), new PrimaryKeySchema("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
            assertEquals(primaryKeySchemaMap.get("pk4"), new PrimaryKeySchema("pk4", PrimaryKeyType.BINARY));
            assertEquals(primaryKeySchemaMap.get("pk5"), new PrimaryKeySchema("pk5", PrimaryKeyType.INTEGER));
            assertEquals(primaryKeySchemaMap.get("pk6"), new PrimaryKeySchema("pk6", PrimaryKeyType.STRING));
            assertEquals(primaryKeySchemaMap.get("pk7"), new PrimaryKeySchema("pk7", PrimaryKeyType.STRING));

            assertEquals(primaryKeyTypeMap.get("pk0"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk1"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk2"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk3"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk4"), PrimaryKeyType.BINARY);
            assertEquals(primaryKeyTypeMap.get("pk5"), PrimaryKeyType.INTEGER);
            assertEquals(primaryKeyTypeMap.get("pk6"), PrimaryKeyType.STRING);
            assertEquals(primaryKeyTypeMap.get("pk7"), PrimaryKeyType.STRING);
        }
    }
}
