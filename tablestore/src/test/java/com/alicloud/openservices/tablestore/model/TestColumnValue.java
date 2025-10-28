package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferInputStream;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestColumnValue {

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

            try {
                v.asStringInBytes();
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
        checkType(v);
    }

    @Test
    public void testString() {
        String value = TestUtil.randomString(100000);
        ColumnValue v = ColumnValue.fromString(value);
        assertEquals(v.asString(), value);
        assertArrayEquals(v.asStringInBytes(), Bytes.toBytes(value));
        checkType(v);
    }

    @Test
    public void testDouble() {
        double value = TestUtil.randomDouble();
        ColumnValue v = ColumnValue.fromDouble(value);
        assertEquals(v.asDouble(), value, 0.000001);
        checkType(v);
    }

    @Test
    public void testBoolean() {
        boolean value = TestUtil.randomBoolean();
        ColumnValue v = ColumnValue.fromBoolean(value);
        assertEquals(v.asBoolean(), value);
        checkType(v);
    }

    @Test
    public void testBytes() {
        byte[] value = TestUtil.randomBytes(100000);
        ColumnValue v = ColumnValue.fromBinary(value);
        assertArrayEquals(v.asBinary(), value);
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

    @Test
    public void testPrimaryKeyAddOne() {
        PrimaryKeyValue pk = PrimaryKeyValue.addOne(PrimaryKeyValue.fromLong(1000));
        System.out.println(pk);
        assertTrue(pk.asLong() == 1001);

        PrimaryKeyValue pk3 = PrimaryKeyValue.addOne(PrimaryKeyValue.fromLong(Long.MAX_VALUE));
        System.out.println(pk3);
        assertTrue(pk3.isInfMax());

        String str = "aaestXXXaa";
        String str1 = "aaestXXXaa\0";
        String str2 = "aaestXXXab";
        String str3 = "aaestXXXaa\1";
        PrimaryKeyValue pk1 = PrimaryKeyValue.addOne(PrimaryKeyValue.fromString(str));
        System.out.println(pk1);
        assertTrue(pk1.asString().equals(str1));
        assertTrue(pk1.compareTo(PrimaryKeyValue.fromString(str2)) < 0);
        assertTrue(pk1.compareTo(PrimaryKeyValue.fromString(str3)) < 0);
        assertTrue(pk1.compareTo(PrimaryKeyValue.fromString(str)) > 0);

        byte[] bstr = Bytes.toBytes("aaestXXXaa");
        byte[] bstr1 = Bytes.toBytes("aaestXXXaa\0");
        byte[] bstr2 = Bytes.toBytes("aaestXXXab");
        byte[] bstr3 = Bytes.toBytes("aaestXXXaa\1");
        PrimaryKeyValue pk2 = PrimaryKeyValue.addOne(PrimaryKeyValue.fromBinary(bstr));
        PrimaryKeyValue pk4 = PrimaryKeyValue.fromBinary(bstr1);
        PrimaryKeyValue pk5 = PrimaryKeyValue.fromBinary(bstr2);
        PrimaryKeyValue pk6 = PrimaryKeyValue.fromBinary(bstr3);
        System.out.println(pk2);
        System.out.println(pk4);
        System.out.println(pk5);
        System.out.println(pk6);
        assertTrue(pk2.compareTo(pk4) == 0);
        assertTrue(pk2.compareTo(pk5) < 0);
        assertTrue(pk2.compareTo(pk6) < 0);
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
        assertTrue(ColumnValue.fromString("??????").compareTo(ColumnValue.fromString("??????")) == 0);
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

    @Test
    public void testGetDataSize() throws Exception {
        assertEquals(ColumnValue.fromBinary(new byte[]{0x0, 0x1, 0x2, 0x3, 0x4, 0x5}).getDataSize(), 6);
        assertEquals(ColumnValue.fromBoolean(true).getDataSize(), 1);
        assertEquals(ColumnValue.fromLong(1).getDataSize(), 8);
        assertEquals(ColumnValue.fromDouble(1.0).getDataSize(), 8);

        String value = "测试";
        assertEquals(ColumnValue.fromString(value).getDataSize(), value.getBytes("utf-8").length);
    }

    @Test
    public void testColumnValue_FromStringWithBytes() throws Exception {
        byte[] bytes = new byte[]{-19, -69, -100};
        String str = PlainBufferInputStream.bytes2UTFString(bytes);
        ColumnValue columnValue1 = ColumnValue.fromString(str);
        ColumnValue columnValue2 = ColumnValue.fromString(str, bytes);

        assertEquals(str, columnValue1.asString());
        assertEquals(str, columnValue2.asString());

        assertFalse(java.util.Arrays.equals(bytes, columnValue1.asStringInBytes()));
        assertArrayEquals(bytes, columnValue2.asStringInBytes());
    }

    @Test
    public void testColumnValue_FromStringWithBytes_2() throws Exception {
        // two different bytes arrays
        // Byte Order Mark
        byte[] bytes1 = new byte[]{-19, -69, -100};
        // Replacement Character
        byte[] bytes2 = new byte[]{ (byte)0xEF, (byte)0xBF, (byte)0xBD };

        String str1 = PlainBufferInputStream.bytes2UTFString(bytes1);
        String str2 = PlainBufferInputStream.bytes2UTFString(bytes2);
        {
            // construct two different ColumnValue with string
            ColumnValue columnValue1 = ColumnValue.fromString(str1);
            ColumnValue columnValue2 = ColumnValue.fromString(str2);

            byte byte1 = columnValue1.getChecksum((byte)0x0);
            byte byte2 = columnValue2.getChecksum((byte)0x0);
            // checksum should be the same
            assertEquals(byte1, byte2);
        }

        {
            // construct two different ColumnValue with string and bytes
            ColumnValue columnValue1 = ColumnValue.fromString(str1, bytes1);
            ColumnValue columnValue2 = ColumnValue.fromString(str2, bytes2);

            byte byte1 = columnValue1.getChecksum((byte)0x0);
            byte byte2 = columnValue2.getChecksum((byte)0x0);
            // checksum should be different
            assertNotSame(byte1, byte2);
        }
    }
}
