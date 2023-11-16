package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
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

        if (type != PrimaryKeyType.DATETIME) {
            try {
                v.asDateTime();
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
    public void testDateTime() {
        ZonedDateTime value = TestUtil.randomDateTime();
        PrimaryKeyValue v = PrimaryKeyValue.fromDateTime(value);
        assertEquals(v.asDateTime().withZoneSameInstant(ZoneId.of("Asia/Shanghai")), value);
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
    public void testEquals_DateTime() {
        ZonedDateTime time1 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123456000, ZoneId.of("Asia/Shanghai"));
        ZonedDateTime time2 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123458000, ZoneId.of("Asia/Shanghai"));
        checkEquals(PrimaryKeyValue.fromDateTime(time1), PrimaryKeyValue.fromDateTime(time1));
        assertTrue(!PrimaryKeyValue.fromDateTime(time1).equals(PrimaryKeyValue.fromDateTime(time2)));
        assertTrue(!PrimaryKeyValue.fromDateTime(time1).equals(PrimaryKeyValue.fromBinary(TestUtil.randomBytes(10))));
        assertTrue(!PrimaryKeyValue.fromDateTime(time1).equals(PrimaryKeyValue.fromString(TestUtil.randomString(10))));
    }

    @Test
    public void testDateTimePrecisionExceed() {
        ZonedDateTime time1 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123458123, ZoneId.of("Asia/Shanghai"));
        try {
            PrimaryKeyValue value = PrimaryKeyValue.fromDateTime(time1);
            fail();
        } catch (RuntimeException e) {

        }
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

        if (type != PrimaryKeyType.DATETIME) {
            try {
                v.compareTo(PrimaryKeyValue.fromDateTime(ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123456000, ZoneId.of("Asia/Shanghai"))));
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
    public void testCompareTo_DATETIME() {
        ZonedDateTime time1 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123456000, ZoneId.of("Asia/Shanghai"));
        ZonedDateTime time2 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123457000, ZoneId.of("Asia/Shanghai"));
        ZonedDateTime time3 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123453000, ZoneId.of("Asia/Shanghai"));
        PrimaryKeyValue value = PrimaryKeyValue.fromDateTime(time1);
        assertTrue(value.compareTo(PrimaryKeyValue.fromDateTime(time1)) == 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromDateTime(time2)) < 0);
        assertTrue(value.compareTo(PrimaryKeyValue.fromDateTime(time3)) > 0);
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
        ZonedDateTime time1 = ZonedDateTime.of(2021, 11, 11, 11, 11, 11, 123456000, ZoneId.of("Asia/Shanghai"));
        assertEquals(PrimaryKeyValue.AUTO_INCREMENT.getDataSize(), 0);
        assertEquals(PrimaryKeyValue.AUTO_INCREMENT.getDataSize(), 0);
        assertEquals(PrimaryKeyValue.fromString("abc").getDataSize(), 3);
        assertEquals(PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}).getDataSize(), 3);
        assertEquals(8, PrimaryKeyValue.fromLong(100).getDataSize());
        assertEquals(8, PrimaryKeyValue.fromDateTime(time1).getDataSize());
    }
}
