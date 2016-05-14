package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import com.aliyun.openservices.ots.utils.Bytes;
import com.aliyun.openservices.ots.utils.TestUtil;
import org.junit.Test;

public class ColumnValueTest {

    private void checkType(ColumnValue v) {
        ColumnType type = v.getType();

        if (type != ColumnType.INTEGER) {
            try {
                v.asLong();
                fail();
            } catch (IllegalStateException e) {

            }
        }

        if (type != ColumnType.STRING) {
            try {
                v.asString();
                fail();
            } catch (IllegalStateException e) {

            }
        }

        if (type != ColumnType.BOOLEAN) {
            try {
                v.asBoolean();
                fail();
            } catch (IllegalStateException e) {

            }
        }

        if (type != ColumnType.BINARY) {
            try {
                v.asBinary();
                fail();
            } catch (IllegalStateException e) {

            }
        }

        if (type != ColumnType.DOUBLE) {
            try {
                v.asDouble();
                fail();
            } catch (IllegalStateException e) {

            }
        }
    }

    @Test
    public void testInteger() {
        long value = TestUtil.randomLong();
        ColumnValue v = ColumnValue.fromLong(value);
        assertEquals(v.asLong(), value);
        assertEquals(v.getSize(), 8);
        checkType(v);
    }

    @Test
    public void testString() {
        String value = TestUtil.randomString(100000);
        ColumnValue v = ColumnValue.fromString(value);
        assertEquals(v.asString(), value);
        assertEquals(v.getSize(), Bytes.toBytes(value).length);
        checkType(v);
    }

    @Test
    public void testDouble() {
        double value = TestUtil.randomDouble();
        ColumnValue v = ColumnValue.fromDouble(value);
        assertEquals(v.asDouble(), value, 0.000001);
        assertEquals(v.getSize(), 8);
        checkType(v);
    }

    @Test
    public void testBoolean() {
        boolean value = TestUtil.randomBoolean();
        ColumnValue v = ColumnValue.fromBoolean(value);
        assertEquals(v.asBoolean(), value);
        assertEquals(v.getSize(), 1);
        checkType(v);
    }

    @Test
    public void testBytes() {
        byte[] value = TestUtil.randomBytes(100000);
        ColumnValue v = ColumnValue.fromBinary(value);
        assertArrayEquals(v.asBinary(), value);
        assertEquals(v.getSize(), value.length);
        checkType(v);
    }

    private void checkEquals(ColumnValue v1, ColumnValue v2) {
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    public void testEquals_Integer() {
        checkEquals(ColumnValue.fromLong(10), ColumnValue.fromLong(10));
        assertTrue(!ColumnValue.fromLong(11).equals(ColumnValue.fromLong(10)));
        assertTrue(!ColumnValue.fromLong(11).equals(ColumnValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!ColumnValue.fromLong(11).equals(ColumnValue.fromBoolean(TestUtil.randomBoolean())));
        assertTrue(!ColumnValue.fromLong(11).equals(ColumnValue.fromDouble(TestUtil.randomDouble())));
        assertTrue(!ColumnValue.fromLong(11).equals(ColumnValue.fromString(TestUtil.randomString(10))));
    }

    @Test
    public void testEquals_Boolean() {
        checkEquals(ColumnValue.fromBoolean(true), ColumnValue.fromBoolean(true));
        assertTrue(!ColumnValue.fromBoolean(true).equals(ColumnValue.fromBoolean(false)));
        assertTrue(!ColumnValue.fromBoolean(true).equals(ColumnValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!ColumnValue.fromBoolean(true).equals(ColumnValue.fromLong(TestUtil.randomLong())));
        assertTrue(!ColumnValue.fromBoolean(true).equals(ColumnValue.fromDouble(TestUtil.randomDouble())));
        assertTrue(!ColumnValue.fromBoolean(true).equals(ColumnValue.fromString(TestUtil.randomString(10))));
    }

    @Test
    public void testEquals_String() {
        checkEquals(ColumnValue.fromString("HelloWorld"), ColumnValue.fromString("HelloWorld"));
        assertTrue(!ColumnValue.fromString("HelloWorld").equals(ColumnValue.fromString("HelloWorld2")));
        assertTrue(!ColumnValue.fromString("HelloWorld").equals(ColumnValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!ColumnValue.fromString("HelloWorld").equals(ColumnValue.fromLong(TestUtil.randomLong())));
        assertTrue(!ColumnValue.fromString("HelloWorld").equals(ColumnValue.fromDouble(TestUtil.randomDouble())));
        assertTrue(!ColumnValue.fromString("HelloWorld").equals(ColumnValue.fromBoolean(false)));
    }

    @Test
    public void testEquals_Double() {
        checkEquals(ColumnValue.fromDouble(0.001), ColumnValue.fromDouble(0.001));
        assertTrue(!ColumnValue.fromDouble(0.001).equals(ColumnValue.fromString("HelloWorld2")));
        assertTrue(!ColumnValue.fromDouble(0.001).equals(ColumnValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!ColumnValue.fromDouble(0.001).equals(ColumnValue.fromLong(TestUtil.randomLong())));
        assertTrue(!ColumnValue.fromDouble(0.001).equals(ColumnValue.fromString("HelloWorld")));
        assertTrue(!ColumnValue.fromDouble(0.001).equals(ColumnValue.fromBoolean(false)));
    }

    @Test
    public void testEquals_Bytes() {
        checkEquals(ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}), ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}));
        assertTrue(!ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(ColumnValue.fromBinary(new byte[]{0x1, 0x3, 0x3})));
        assertTrue(!ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(ColumnValue.fromDouble(TestUtil.randomDouble())));
        assertTrue(!ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(ColumnValue.fromLong(TestUtil.randomLong())));
        assertTrue(!ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(ColumnValue.fromString("HelloWorld")));
        assertTrue(!ColumnValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(ColumnValue.fromBoolean(false)));
    }

    private void compareWithOtherType(ColumnValue v) {
        ColumnType type = v.getType();

        if (type != ColumnType.INTEGER) {
            try {
                v.compareTo(ColumnValue.fromLong(10));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != ColumnType.STRING) {
            try {
                v.compareTo(ColumnValue.fromString("HelloWorld"));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != ColumnType.BOOLEAN) {
            try {
                v.compareTo(ColumnValue.fromBoolean(true));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != ColumnType.BINARY) {
            try {
                v.compareTo(ColumnValue.fromBinary(new byte[]{0x1, 0x2}));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != ColumnType.DOUBLE) {
            try {
                v.compareTo(ColumnValue.fromDouble(10.0));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }
    }

    @Test
    public void testCompareTo_Integer() {
        ColumnValue value = ColumnValue.fromLong(0);
        assertTrue(value.compareTo(ColumnValue.fromLong(0)) == 0);
        assertTrue(value.compareTo(ColumnValue.fromLong(1)) < 0);
        assertTrue(value.compareTo(ColumnValue.fromLong(-1)) > 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_String() {
        ColumnValue value = ColumnValue.fromString("a b c d e");
        assertTrue(value.compareTo(ColumnValue.fromString("a b c d e")) == 0);
        assertTrue(ColumnValue.fromString("����Ͱ�").compareTo(ColumnValue.fromString("����Ͱ�")) == 0);
        assertTrue(value.compareTo(ColumnValue.fromString("a b c d f")) < 0);
        assertTrue(value.compareTo(ColumnValue.fromString("a b c d d")) > 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Bytes() {
        ColumnValue value = ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3});
        assertTrue(value.compareTo(ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3})) == 0);
        assertTrue(value.compareTo(ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xfe, 0x3})) > 0);
        assertTrue(value.compareTo(ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x4})) < 0);
        assertTrue(value.compareTo(ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff})) > 0);
        assertTrue(value.compareTo(ColumnValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3, 0x0})) < 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Double() {
        ColumnValue value = ColumnValue.fromDouble(10.0);
        assertTrue(value.compareTo(ColumnValue.fromDouble(10.0)) == 0);
        assertTrue(value.compareTo(ColumnValue.fromDouble(9.9)) > 0);
        assertTrue(value.compareTo(ColumnValue.fromDouble(10.1)) < 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Boolean() {
        ColumnValue value = ColumnValue.fromBoolean(false);
        assertTrue(value.compareTo(ColumnValue.fromBoolean(false)) == 0);
        assertTrue(value.compareTo(ColumnValue.fromBoolean(true)) < 0);

        compareWithOtherType(value);
    }

}
