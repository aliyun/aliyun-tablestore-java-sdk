package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPrimaryKeyColumn {

    @Test
    public void testConstructor() {
        PrimaryKeyColumn pk = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1912));
        assertEquals(pk.getName(), "PK1");
        assertEquals(pk.getValue(), PrimaryKeyValue.fromLong(1912));

        try {
            new PrimaryKeyColumn(null, PrimaryKeyValue.fromLong(1912));
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            new PrimaryKeyColumn("", PrimaryKeyValue.fromLong(1912));
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            new PrimaryKeyColumn("PK1", null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testEquals() {
        PrimaryKeyColumn pk1 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1912));
        PrimaryKeyColumn pk2 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1912));

        // value not equal
        PrimaryKeyColumn pk3 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1911));

        // name not equal
        PrimaryKeyColumn pk4 = new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1912));
        assertEquals(pk1, pk2);
        assertEquals(pk1.hashCode(), pk2.hashCode());

        assertTrue(!pk1.equals(pk3));
        assertTrue(pk1.hashCode() != pk3.hashCode());
        assertTrue(!pk1.equals(pk4));
        assertTrue(pk1.hashCode() != pk4.hashCode());
    }

    @Test
    public void testCompareTo() {
        PrimaryKeyColumn pk1 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1912));
        PrimaryKeyColumn pk2 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1912));
        assertTrue(pk1.compareTo(pk2) == 0);

        PrimaryKeyColumn pk3 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1911));
        PrimaryKeyColumn pk4 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1913));

        assertTrue(pk1.compareTo(pk3) > 0);
        assertTrue(pk1.compareTo(pk4) < 0);

        // name not equal
        try {
            PrimaryKeyColumn pk5 = new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1912));
            pk1.compareTo(pk5);
            fail();
        } catch (IllegalArgumentException e) {

        }

        // value type not equal
        try {
            PrimaryKeyColumn pk6 = new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("1912"));
            pk1.compareTo(pk6);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testGetDataSize() {
        PrimaryKeyColumn pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.fromString("abc"));
        assertEquals(pkc.getDataSize(), 6);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1}));
        assertEquals(pkc.getDataSize(), 5);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(100));
        assertEquals(pkc.getDataSize(), 11);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MIN);
        assertEquals(pkc.getDataSize(), 3);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.INF_MAX);
        assertEquals(pkc.getDataSize(), 3);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.AUTO_INCREMENT);
        assertEquals(pkc.getDataSize(), 3);

        pkc = new PrimaryKeyColumn("pk0", PrimaryKeyValue.AUTO_INCRMENT);
        assertEquals(pkc.getDataSize(), 3);
    }
}
