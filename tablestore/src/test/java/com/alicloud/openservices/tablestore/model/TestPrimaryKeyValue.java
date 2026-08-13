package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferInputStream;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestPrimaryKeyValue {

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

            try {
                v.asStringInBytes();
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

        if (type != PrimaryKeyType.BOOLEAN) {
            try {
                v.asBoolean();
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
        checkType(v);
    }

    @Test
    public void testString() {
        String value = TestUtil.randomString(100000);
        PrimaryKeyValue v = PrimaryKeyValue.fromString(value);
        assertEquals(v.asString(), value);
        Assert.assertArrayEquals(v.asStringInBytes(), Bytes.toBytes(value));
        checkType(v);
    }

    @Test
    public void testBytes() {
        byte[] value = TestUtil.randomBytes(100000);
        PrimaryKeyValue v = PrimaryKeyValue.fromBinary(value);
        assertArrayEquals(v.asBinary(), value);
        checkType(v);
    }

    @Test
    public void testBoolean() {
        PrimaryKeyValue t = PrimaryKeyValue.fromBoolean(true);
        assertEquals(PrimaryKeyType.BOOLEAN, t.getType());
        assertTrue(t.asBoolean());
        assertEquals("true", t.toString());
        checkType(t);

        PrimaryKeyValue f = PrimaryKeyValue.fromBoolean(false);
        assertEquals(PrimaryKeyType.BOOLEAN, f.getType());
        assertFalse(f.asBoolean());
        assertEquals("false", f.toString());
        checkType(f);
    }

    private void checkEquals(PrimaryKeyValue v1, PrimaryKeyValue v2) {
        assertEquals(v1, v2);
        assertEquals(v1.hashCode(), v2.hashCode());
    }

    @Test
    public void testEquals_Integer() {
        checkEquals(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromLong(10));
        assertTrue(!PrimaryKeyValue.fromLong(11).equals(PrimaryKeyValue.fromLong(10)));
        assertTrue(!PrimaryKeyValue.fromLong(11).equals(PrimaryKeyValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!PrimaryKeyValue.fromLong(11).equals(PrimaryKeyValue.fromString(TestUtil.randomString(10))));
    }

    @Test
    public void testEquals_String() {
        checkEquals(PrimaryKeyValue.fromString("HelloWorld"), PrimaryKeyValue.fromString("HelloWorld"));
        assertTrue(!PrimaryKeyValue.fromString("HelloWorld").equals(PrimaryKeyValue.fromString("HelloWorld2")));
        assertTrue(!PrimaryKeyValue.fromString("HelloWorld").equals(PrimaryKeyValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!PrimaryKeyValue.fromString("HelloWorld").equals(PrimaryKeyValue.fromLong(TestUtil.randomLong())));
    }

    @Test
    public void testEquals_Bytes() {
        checkEquals(PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2, 0x3}), PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2, 0x3}));
        assertTrue(!PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x3, 0x3})));
        assertTrue(!PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(PrimaryKeyValue.fromLong(TestUtil.randomLong())));
        assertTrue(!PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2, 0x3}).equals(PrimaryKeyValue.fromString("HelloWorld")));
    }

    @Test
    public void testEquals_Boolean() {
        checkEquals(PrimaryKeyValue.fromBoolean(true), PrimaryKeyValue.fromBoolean(true));
        checkEquals(PrimaryKeyValue.fromBoolean(false), PrimaryKeyValue.fromBoolean(false));
        assertTrue(!PrimaryKeyValue.fromBoolean(true).equals(PrimaryKeyValue.fromBoolean(false)));
        assertTrue(!PrimaryKeyValue.fromBoolean(true).equals(PrimaryKeyValue.fromLong(1)));
        assertTrue(!PrimaryKeyValue.fromBoolean(true).equals(PrimaryKeyValue.fromString("true")));
    }

    @Test
    public void testEquals_AUTOINCREMENT() {
        checkEquals(PrimaryKeyValue.AUTO_INCREMENT, PrimaryKeyValue.AUTO_INCRMENT);
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
                v.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x1, 0x2}));
                fail();
            } catch (IllegalArgumentException e) {

            }
        }

        if (type != PrimaryKeyType.BOOLEAN) {
            try {
                v.compareTo(PrimaryKeyValue.fromBoolean(true));
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
        assertTrue(PrimaryKeyValue.fromString("����Ͱ�").compareTo(PrimaryKeyValue.fromString("����Ͱ�")) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromString("a b c d f")) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromString("a b c d d")) > 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Bytes() {
        PrimaryKeyValue value = PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3});
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3})) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xfe, 0x3})) > 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x4})) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff})) > 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, (byte)0xff, 0x3, 0x0})) < 0);

        compareWithOtherType(value);
    }

    @Test
    public void testCompareTo_Boolean() {
        PrimaryKeyValue value = PrimaryKeyValue.fromBoolean(false);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBoolean(false)) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromBoolean(true)) < 0);
        assertTrue(PrimaryKeyValue.fromBoolean(true).compareTo(PrimaryKeyValue.fromBoolean(false)) > 0);
        assertTrue(PrimaryKeyValue.fromBoolean(true).compareTo(PrimaryKeyValue.fromBoolean(true)) == 0);

        compareWithOtherType(value);
    }

    @Test
    public void testFromColumn() {
        ColumnValue column = ColumnValue.fromString("hello world");
        PrimaryKeyValue pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asString(), column.asString());

        column = ColumnValue.fromBinary(new byte[]{0xa, 0xb, 0xc, 0xd, 0xe});
        pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asBinary(), column.asBinary());

        column = ColumnValue.fromLong(1024);
        pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asLong(), column.asLong());

        column = ColumnValue.fromBoolean(false);
        pk = PrimaryKeyValue.fromColumn(column);
        assertEquals(pk.asBoolean(), column.asBoolean());

        column = ColumnValue.fromDouble(1024);
        try {
            PrimaryKeyValue.fromColumn(column);
            fail();
        } catch(IllegalArgumentException e) {

        }
    }

    @Test
    public void testHashCode() {
        Map<PrimaryKeyValue, Long> valueMap = new HashMap<PrimaryKeyValue, Long>();
        {
            PrimaryKeyValue value = PrimaryKeyValue.AUTO_INCREMENT;
            valueMap.put(value, 100L);
        }

        {
            PrimaryKeyValue value = PrimaryKeyValue.INF_MAX;
            valueMap.put(value, 100L);
        }

        {
            PrimaryKeyValue value = PrimaryKeyValue.INF_MIN;
            valueMap.put(value, 100L);
        }
    }

    @Test
    public void testGetDataSize() {
        assertEquals(PrimaryKeyValue.AUTO_INCREMENT.getDataSize(), 0);
        assertEquals(PrimaryKeyValue.AUTO_INCREMENT.getDataSize(), 0);
        assertEquals(PrimaryKeyValue.fromString("abc").getDataSize(), 3);
        assertEquals(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}).getDataSize(), 3);
        assertEquals(8, PrimaryKeyValue.fromLong(100).getDataSize());
        assertEquals(1, PrimaryKeyValue.fromBoolean(true).getDataSize());
    }

    @Test
    public void testPrimaryKeyValue_FromStringWithBytes() throws Exception {
        byte[] bytes = new byte[]{-19, -69, -100};
        String str = PlainBufferInputStream.bytes2UTFString(bytes);
        PrimaryKeyValue primaryKeyValue1 = PrimaryKeyValue.fromString(str);
        PrimaryKeyValue primaryKeyValue2 = PrimaryKeyValue.fromString(str, bytes);

        assertEquals(str, primaryKeyValue1.asString());
        assertEquals(str, primaryKeyValue2.asString());

        assertFalse(java.util.Arrays.equals(bytes, primaryKeyValue1.asStringInBytes()));
        assertArrayEquals(bytes, primaryKeyValue2.asStringInBytes());
    }
}
