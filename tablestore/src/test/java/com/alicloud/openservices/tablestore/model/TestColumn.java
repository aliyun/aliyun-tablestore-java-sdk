package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestColumn {

    @Test
    public void testConstructor() {
        Column column = new Column("Column1", ColumnValue.fromLong(1000));
        assertEquals(column.getName(), "Column1");
        assertEquals(column.getValue(), ColumnValue.fromLong(1000));
        assertTrue(!column.hasSetTimestamp());

        column = new Column("Column2", ColumnValue.fromString("ColumnValue"), 1418380771);
        assertEquals(column.getName(), "Column2");
        assertEquals(column.getValue(), ColumnValue.fromString("ColumnValue"));
        assertTrue(column.hasSetTimestamp());
        assertEquals(column.getTimestamp(), 1418380771);
    }

    @Test
    public void testConstructor_InvalidArguments() {
        try {
            new Column(null, ColumnValue.fromString("C"));
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new Column("", ColumnValue.fromString("C"));
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new Column("T", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new Column("T", ColumnValue.fromString("C"), -1);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testEquals() {
        Column c1 = new Column("T1", ColumnValue.fromLong(1), 1418380771);
        Column c2 = new Column("T1", ColumnValue.fromLong(1), 1418380771);
        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());

        c1 = new Column("T1", ColumnValue.fromLong(1));
        c2 = new Column("T1", ColumnValue.fromLong(1));
        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());

        // name not same
        c1 = new Column("T1", ColumnValue.fromLong(1), 1418380771);
        c2 = new Column("T2", ColumnValue.fromLong(1), 1418380771);
        assertTrue(!c1.equals(c2));
        assertTrue(c1.hashCode() != c2.hashCode());

        // value not same
        c1 = new Column("T1", ColumnValue.fromLong(1), 1418380771);
        c2 = new Column("T1", ColumnValue.fromLong(2), 1418380771);
        assertTrue(!c1.equals(c2));
        assertTrue(c1.hashCode() != c2.hashCode());


        // timestamp not same
        c1 = new Column("T1", ColumnValue.fromLong(1), 1418380771);
        c2 = new Column("T1", ColumnValue.fromLong(1), 1418380772);
        assertTrue(!c1.equals(c2));
        assertTrue(c1.hashCode() != c2.hashCode());

        c1 = new Column("T1", ColumnValue.fromLong(1));
        c2 = new Column("T1", ColumnValue.fromLong(1), 1418380772);
        assertTrue(!c1.equals(c2));
        assertTrue(c1.hashCode() != c2.hashCode());
    }

    @Test
    public void testGetDataSize() {
        Column column = new Column("column", ColumnValue.fromString("abc"));
        assertEquals(column.getDataSize(), 9);

        column = new Column("column", ColumnValue.fromDouble(0.0));
        assertEquals(column.getDataSize(), 14);

        column = new Column("column", ColumnValue.fromLong(1));
        assertEquals(column.getDataSize(), 14);

        column = new Column("column", ColumnValue.fromBoolean(false));
        assertEquals(column.getDataSize(), 7);

        column = new Column("column", ColumnValue.fromBinary(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4}));
        assertEquals(column.getDataSize(), 11);

        column = new Column("column", ColumnValue.fromString("abc"), System.currentTimeMillis());
        assertEquals(column.getDataSize(), 17);

        column = new Column("column", ColumnValue.fromDouble(0.0), System.currentTimeMillis());
        assertEquals(column.getDataSize(), 22);

        column = new Column("column", ColumnValue.fromLong(1), System.currentTimeMillis());
        assertEquals(column.getDataSize(), 22);

        column = new Column("column", ColumnValue.fromBoolean(false), System.currentTimeMillis());
        assertEquals(column.getDataSize(), 15);

        column = new Column("column", ColumnValue.fromBinary(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4}), System.currentTimeMillis());
        assertEquals(column.getDataSize(), 19);
    }
}
