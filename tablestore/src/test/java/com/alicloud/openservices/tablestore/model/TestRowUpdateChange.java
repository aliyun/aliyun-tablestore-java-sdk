package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestRowUpdateChange {

    @Test
    public void testConstructor() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowUpdateChange rowChange = new RowUpdateChange("T", primaryKey);

        assertEquals(rowChange.getTableName(), "T");
        assertEquals(rowChange.getPrimaryKey(), primaryKey);
        assertTrue(rowChange.getColumnsToUpdate().isEmpty());
    }

    @Test
    public void testOperations() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowUpdateChange rowChange = new RowUpdateChange("T", primaryKey);

        Column column1 = new Column("Column0", ColumnValue.fromLong(1912), 1418647314);
        rowChange.put(column1);
        assertEquals(rowChange.getColumnsToUpdate().size(), 1);
        assertEquals(rowChange.getColumnsToUpdate().get(0).getSecond(), RowUpdateChange.Type.PUT);
        assertEquals(rowChange.getColumnsToUpdate().get(0).getFirst(), column1);

        rowChange.put("Column1", ColumnValue.fromString("Column Value"));
        assertEquals(rowChange.getColumnsToUpdate().size(), 2);
        assertEquals(rowChange.getColumnsToUpdate().get(1).getSecond(), RowUpdateChange.Type.PUT);
        assertEquals(rowChange.getColumnsToUpdate().get(1).getFirst(), new Column("Column1", ColumnValue.fromString("Column Value")));

        rowChange.put("Column2", ColumnValue.fromString("Column Value"), 1418647315);
        assertEquals(rowChange.getColumnsToUpdate().size(), 3);
        assertEquals(rowChange.getColumnsToUpdate().get(2).getSecond(), RowUpdateChange.Type.PUT);
        assertEquals(rowChange.getColumnsToUpdate().get(2).getFirst(), new Column("Column2", ColumnValue.fromString("Column Value"), 1418647315));

        rowChange.deleteColumn("Column1", 1418647312);
        assertEquals(rowChange.getColumnsToUpdate().size(), 4);
        assertEquals(rowChange.getColumnsToUpdate().get(3).getSecond(), RowUpdateChange.Type.DELETE);
        assertEquals(rowChange.getColumnsToUpdate().get(3).getFirst().getName(), "Column1");
        assertEquals(rowChange.getColumnsToUpdate().get(3).getFirst().getTimestamp(), 1418647312);

        rowChange.deleteColumns("Column2");
        assertEquals(rowChange.getColumnsToUpdate().size(), 5);
        assertEquals(rowChange.getColumnsToUpdate().get(4).getSecond(), RowUpdateChange.Type.DELETE_ALL);
        assertEquals(rowChange.getColumnsToUpdate().get(4).getFirst().getName(), "Column2");
    }

    @Test
    public void testPutList() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowUpdateChange rowChange = new RowUpdateChange("T", primaryKey);

        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) {
                columns.add(new Column("Column" + i, ColumnValue.fromLong(i)));
            } else {
                columns.add(new Column("Column" + i, ColumnValue.fromLong(i), 1418647300 + i));
            }
        }

        rowChange.put(columns);

        assertEquals(rowChange.getColumnsToUpdate().size(), columns.size());
        for (int i = 0; i < columns.size(); i++) {
            assertEquals(rowChange.getColumnsToUpdate().get(i).getSecond(), RowUpdateChange.Type.PUT);
            assertEquals(rowChange.getColumnsToUpdate().get(i).getFirst(), columns.get(i));
        }
    }

    @Test
    public void testConstructorWithInvalidArguments() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowUpdateChange rowChange = new RowUpdateChange("T", primaryKey);
        assertEquals(rowChange.getTableName(), "T");
        assertEquals(rowChange.getPrimaryKey(), primaryKey);

        try {
            new RowUpdateChange("", primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowUpdateChange(null, primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowUpdateChange("T", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowUpdateChange("T", PrimaryKeyBuilder.createPrimaryKeyBuilder().build());
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
        RowUpdateChange rowChange = new RowUpdateChange("T", primaryKey);
        RowUpdateChange rowChange2 = new RowUpdateChange("T", primaryKey2);

        assertTrue(rowChange.compareTo(rowChange2) == 0);
    }

    @Test
    public void testGetDataSize() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(8))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abcde"))
                .build();

        RowUpdateChange ruc = new RowUpdateChange("TestTable", primaryKey);
        ruc.deleteColumns("col0");
        ruc.deleteColumn("col1", System.currentTimeMillis());
        ruc.put("col2", ColumnValue.fromString("abcde"));
        ruc.put("col3", ColumnValue.fromString("abcde"), System.currentTimeMillis());

        assertEquals(ruc.getDataSize(), 61);
    }
}
