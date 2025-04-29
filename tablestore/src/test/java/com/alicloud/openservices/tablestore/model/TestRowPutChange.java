package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestRowPutChange {

    @Test
    public void testAddColumn_WithDefaultTimestamp() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();

        RowPutChange rowChange = new RowPutChange("T", primaryKey, 1418630000);

        rowChange.addColumn("Column0", ColumnValue.fromLong(10));

        Column target = rowChange.getColumnsToPut().get(0);
        assertEquals(target.getName(), "Column0");
        assertEquals(target.getValue(), ColumnValue.fromLong(10));
        assertTrue(target.hasSetTimestamp());
        assertEquals(target.getTimestamp(), 1418630000);
        assertEquals(rowChange.getReturnType(), ReturnType.RT_NONE);

        rowChange.addColumn("Column0", ColumnValue.fromLong(11), 1418630002);
        target = rowChange.getColumnsToPut().get(1);
        assertEquals(target.getName(), "Column0");
        assertEquals(target.getValue(), ColumnValue.fromLong(11));
        assertTrue(target.hasSetTimestamp());
        assertEquals(target.getTimestamp(), 1418630002);

        rowChange.addColumn(new Column("Column1", ColumnValue.fromLong(12)));
        target = rowChange.getColumnsToPut().get(2);
        assertEquals(target.getName(), "Column1");
        assertEquals(target.getValue(), ColumnValue.fromLong(12));
        assertTrue(!target.hasSetTimestamp());
    }

    @Test
    public void testAddColumn_WithReturnType() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();

        RowPutChange rowChange = new RowPutChange("T", primaryKey, 1418630000);
        rowChange.setReturnType(ReturnType.RT_PK);

        rowChange.addColumn("Column0", ColumnValue.fromLong(10));

        Column target = rowChange.getColumnsToPut().get(0);
        assertEquals(target.getName(), "Column0");
        assertEquals(target.getValue(), ColumnValue.fromLong(10));
        assertTrue(target.hasSetTimestamp());
        assertEquals(target.getTimestamp(), 1418630000);
        assertEquals(rowChange.getReturnType(), ReturnType.RT_PK);
    }

    @Test
    public void testAddColumn_WithoutDefaultTimestamp() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();

        RowPutChange rowChange = new RowPutChange("T", primaryKey);

        rowChange.addColumn("Column0", ColumnValue.fromLong(10));

        Column target = rowChange.getColumnsToPut().get(0);
        assertEquals(target.getName(), "Column0");
        assertEquals(target.getValue(), ColumnValue.fromLong(10));
        assertTrue(!target.hasSetTimestamp());

        rowChange.addColumn("Column0", ColumnValue.fromLong(11), 1418630002);
        target = rowChange.getColumnsToPut().get(1);
        assertEquals(target.getName(), "Column0");
        assertEquals(target.getValue(), ColumnValue.fromLong(11));
        assertTrue(target.hasSetTimestamp());
        assertEquals(target.getTimestamp(), 1418630002);

        rowChange.addColumn(new Column("Column1", ColumnValue.fromLong(12)));
        target = rowChange.getColumnsToPut().get(2);
        assertEquals(target.getName(), "Column1");
        assertEquals(target.getValue(), ColumnValue.fromLong(12));
        assertTrue(!target.hasSetTimestamp());
    }

    @Test
    public void testAddColumns() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();

        RowPutChange rowChange = new RowPutChange("T", primaryKey);

        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                columns.add(new Column("Column" + i, ColumnValue.fromLong(i)));
            } else {
                columns.add(new Column("Column" + i, ColumnValue.fromLong(i), 1418630000 + i));
            }
        }
        rowChange.addColumns(columns);

        assertEquals(rowChange.getColumnsToPut().size(), 10);
        for (int i = 0; i < 10; i++) {
            assertEquals(rowChange.getColumnsToPut().get(i), columns.get(i));
        }

        for (int i = 0; i < 10; i++) {
            String columnName = "Column" + i;
            assertTrue(rowChange.has(columnName));
            assertTrue(!rowChange.has("Column_" + i));

            if (i % 2 != 0) {
                assertTrue(rowChange.has(columnName, 1418630000 + i));
                assertTrue(!rowChange.has(columnName, 1418630000 + i + 1));
                assertTrue(rowChange.has(columnName, 1418630000 + i, ColumnValue.fromLong(i)));
                assertTrue(!rowChange.has(columnName, 1418630000 + i + 1, ColumnValue.fromLong(i)));
                assertTrue(!rowChange.has(columnName, 1418630000 + i, ColumnValue.fromLong(i + 1)));
            } else {
                assertTrue(!rowChange.has(columnName, 1418630000 + i));
                assertTrue(rowChange.has(columnName, ColumnValue.fromLong(i)));
                assertTrue(!rowChange.has(columnName, ColumnValue.fromLong(i + 1)));
            }

            assertEquals(rowChange.getColumnsToPut(columnName).size(), 1);
            assertEquals(rowChange.getColumnsToPut(columnName).get(0).getName(), columnName);
            assertEquals(rowChange.getColumnsToPut(columnName).get(0).getValue(), ColumnValue.fromLong(i));

            if (i % 2 == 0) {
                assertTrue(!rowChange.getColumnsToPut(columnName).get(0).hasSetTimestamp());
            } else {
                assertTrue(rowChange.getColumnsToPut(columnName).get(0).hasSetTimestamp());
                assertEquals(rowChange.getColumnsToPut(columnName).get(0).getTimestamp(), 1418630000 + i);
            }
        }
    }

    @Test
    public void testConstructorWithInvalidArguments() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        RowPutChange rowChange = new RowPutChange("T", primaryKey);
        assertEquals(rowChange.getTableName(), "T");
        assertEquals(rowChange.getPrimaryKey(), primaryKey);

        try {
            new RowPutChange("", primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowPutChange(null, primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowPutChange("T", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new RowPutChange("T", PrimaryKeyBuilder.createPrimaryKeyBuilder().build());
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
        RowPutChange rowChange = new RowPutChange("T", primaryKey);
        RowPutChange rowChange2 = new RowPutChange("T", primaryKey2);

        assertTrue(rowChange.compareTo(rowChange2) == 0);
    }

    @Test
    public void testGetDataSize() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(11))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abcde"))
                .build();

        RowPutChange rpc = new RowPutChange("TestTable", primaryKey);
        rpc.addColumn("col0", ColumnValue.fromString("abcde"));
        rpc.addColumn("col1", ColumnValue.fromString("abcde"), System.currentTimeMillis());
        rpc.addColumn(new Column("col2", ColumnValue.fromBinary(new byte[]{0x0, 0x1, 0x2})));
        rpc.addColumns(new Column[]{new Column("col3", ColumnValue.fromBoolean(false))});
        rpc.addColumns(Arrays.asList(new Column("col3", ColumnValue.fromLong(1))));

        assertEquals(rpc.getDataSize(), 69);
    }
}
