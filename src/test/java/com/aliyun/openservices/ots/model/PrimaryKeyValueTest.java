package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import com.aliyun.openservices.ots.utils.Bytes;
import com.aliyun.openservices.ots.utils.TestUtil;
import org.junit.Assert;
import org.junit.Test;

public class PrimaryKeyValueTest {
    
    @Test
    public void testVariousType() {
        PrimaryKeyValue value = PrimaryKeyValue.fromLong(999);
        assertEquals(value.asLong(), 999);
        assertEquals(value.getType(), PrimaryKeyType.INTEGER);
        assertTrue(value.equals(PrimaryKeyValue.fromLong(value.asLong())));
        try {
            value.asString();
            fail();
        } catch(Exception e) {}
        
        value = PrimaryKeyValue.fromString("OTS");
        assertEquals(value.asString(), "OTS");
        assertEquals(value.getType(), PrimaryKeyType.STRING);
        assertTrue(value.equals(PrimaryKeyValue.fromString(value.asString())));
        try {
            value.asLong();
            fail();
        } catch(Exception e) {}
    }

    private void checkType(PrimaryKeyValue v) {
        PrimaryKeyType type = v.getType();

        if (type != PrimaryKeyType.INTEGER) {
            try {
                v.asLong();
                fail();
            } catch (IllegalStateException e) {

            }
        }

        if (type != PrimaryKeyType.STRING) {
            try {
                v.asString();
                fail();
            } catch (IllegalStateException e) {

            }

        }

        if (type != PrimaryKeyType.BINARY) {
            try {
                v.asBinary();
                fail();
            } catch (IllegalStateException e) {

            }
        }
    }

    @Test
    public void testInteger() {
        long value = TestUtil.randomLong();
        PrimaryKeyValue v = PrimaryKeyValue.fromLong(value);
        assertEquals(v.asLong(), value);
        assertEquals(v.getSize(), 8);
        checkType(v);
    }

    @Test
    public void testString() {
        String value = TestUtil.randomString(100000);
        PrimaryKeyValue v = PrimaryKeyValue.fromString(value);
        assertEquals(v.asString(), value);
        assertEquals(v.getSize(), Bytes.toBytes(value).length);
        checkType(v);
    }

    @Test
    public void testBinary() {
        byte[] value = TestUtil.randomBytes(100000);
        PrimaryKeyValue v = PrimaryKeyValue.fromBinary(value);
        assertEquals(v.asBinary(), value);
        assertEquals(v.getSize(), value.length);
        checkType(v);
    }

    private void checkEquals(PrimaryKeyValue v1, PrimaryKeyValue v2) {
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    public void testEquals_Integer() {
        checkEquals(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(10));
        assertTrue(!PrimaryKeyValue.fromLong(11).equals(PrimaryKeyValue.fromLong(10)));
        assertTrue(!PrimaryKeyValue.fromLong(11).equals(PrimaryKeyValue.fromString(TestUtil.randomString(10))));
    }

    @Test
     public void testEquals_String() {
        checkEquals(PrimaryKeyValue.fromString("HelloWorld"), PrimaryKeyValue.fromString("HelloWorld"));
        assertTrue(!PrimaryKeyValue.fromString("HelloWorld").equals(PrimaryKeyValue.fromString("HelloWorld2")));
        assertTrue(!PrimaryKeyValue.fromString("HelloWorld").equals(PrimaryKeyValue.fromLong(TestUtil.randomLong())));
    }

    @Test
    public void testEquals_Binary() {
        checkEquals(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x3, 0x10}), PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x3, 0x10}));
        assertTrue(!PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2, 0x10}).equals(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x2, 0x2, 0x10})));
        assertTrue(!PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2, 0x10}).equals(PrimaryKeyValue.fromLong(TestUtil.randomLong())));
    }

    private void compareWithOtherType(PrimaryKeyValue v) {
        PrimaryKeyType type = v.getType();

        if (type != PrimaryKeyType.INTEGER) {
            try {
                v.compareTo(PrimaryKeyValue.fromLong(10));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != PrimaryKeyType.STRING) {
            try {
                v.compareTo(PrimaryKeyValue.fromString("HelloWorld"));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != PrimaryKeyType.BINARY) {
            try {
                v.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0}));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }
    }

    @Test
    public void testCompareTo_Integer() {
        PrimaryKeyValue value = PrimaryKeyValue.fromLong(0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromLong(0)) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromLong(1)) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromLong(-1)) > 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_String() {
        PrimaryKeyValue value = PrimaryKeyValue.fromString("a b c d e");
        assertTrue(value.compareTo(PrimaryKeyValue.fromString("a b c d e")) == 0);
        assertTrue(PrimaryKeyValue.fromString("阿里巴巴").compareTo(PrimaryKeyValue.fromString("阿里巴巴")) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromString("a b c d f")) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromString("a b c d d")) > 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Bytes() {
        PrimaryKeyValue value = PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte) 0xff, 0x3});
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3})) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte) 0xfe, 0x3})) > 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte) 0xff, 0x4})) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte) 0xff})) > 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte) 0xff, 0x3, 0x0})) < 0);

        compareWithOtherType(value);
    }

    @Test
    public void testFromColumn() {
        ColumnValue column = ColumnValue.fromString("hello world");
        PrimaryKeyValue pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asString(), column.asString());

        column = ColumnValue.fromLong(1024);
        pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asLong(), column.asLong());

        column = ColumnValue.fromBinary(new byte[]{0x0, 0x1, 0x2});
        pk = PrimaryKeyValue.fromColumn(column);
        assertArrayEquals(pk.asBinary(), column.asBinary());

        column = ColumnValue.fromDouble(1024);
        try {
            PrimaryKeyValue.fromColumn(column);
            fail();
        } catch(IllegalArgumentException e) {

        }

        column = ColumnValue.fromBoolean(false);
        try {
            PrimaryKeyValue.fromColumn(column);
            fail();
        } catch(IllegalArgumentException e) {

        }
    }

}
