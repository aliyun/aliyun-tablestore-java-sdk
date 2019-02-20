package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCapacityUnit {

    @Test
    public void testConstructor_ReadWriteNotSet() {
        CapacityUnit cu = new CapacityUnit();

        assertTrue(!cu.hasSetReadCapacityUnit());
        assertTrue(!cu.hasSetWriteCapacityUnit());

        try {
            cu.getReadCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }
        try {
            cu.getWriteCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }

        CapacityUnit cu2 = new CapacityUnit();
        assertEquals(cu, cu2);
        assertEquals(cu.hashCode(), cu2.hashCode());

        cu2.setReadCapacityUnit(1);
        assertEquals(cu2.getReadCapacityUnit(), 1);
        assertTrue(!cu.equals(cu2));
        assertTrue(cu.hashCode() != cu2.hashCode());
    }

    @Test
    public void testConstructor_ReadWriteSet() {
        CapacityUnit cu = new CapacityUnit(1, 10);

        assertTrue(cu.hasSetReadCapacityUnit());
        assertTrue(cu.hasSetWriteCapacityUnit());

        assertEquals(cu.getReadCapacityUnit(), 1);
        assertEquals(cu.getWriteCapacityUnit(), 10);

        CapacityUnit cu2 = new CapacityUnit(1, 10);
        assertEquals(cu, cu2);
        assertEquals(cu.hashCode(), cu2.hashCode());

        cu2.setReadCapacityUnit(2);
        assertEquals(cu2.getReadCapacityUnit(), 2);
        assertTrue(!cu.equals(cu2));
        assertTrue(cu.hashCode() != cu2.hashCode());
    }

    @Test
    public void testGetSetRead() {
        CapacityUnit cu = new CapacityUnit();
        cu.setReadCapacityUnit(100);
        assertTrue(cu.hasSetReadCapacityUnit());
        assertEquals(cu.getReadCapacityUnit(), 100);
        assertTrue(!cu.hasSetWriteCapacityUnit());
        try {
            cu.getWriteCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }

        cu.setReadCapacityUnit(999);
        assertTrue(cu.hasSetReadCapacityUnit());
        assertEquals(cu.getReadCapacityUnit(), 999);
        assertTrue(!cu.hasSetWriteCapacityUnit());
        try {
            cu.getWriteCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }
    }

    @Test
    public void testGetSetWrite() {
        CapacityUnit cu = new CapacityUnit();
        cu.setWriteCapacityUnit(100);
        assertTrue(cu.hasSetWriteCapacityUnit());
        assertEquals(cu.getWriteCapacityUnit(), 100);
        assertTrue(!cu.hasSetReadCapacityUnit());
        try {
            cu.getReadCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }

        cu.setWriteCapacityUnit(999);
        assertTrue(cu.hasSetWriteCapacityUnit());
        assertEquals(cu.getWriteCapacityUnit(), 999);
        assertTrue(!cu.hasSetReadCapacityUnit());
        try {
            cu.getReadCapacityUnit();
            fail();
        } catch (IllegalStateException e) {

        }
    }

    private void checkInvalidConstructor(int read, int write) {
        try {
            new CapacityUnit(read, write);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testInvalidArguments() {
        checkInvalidConstructor(-1, 1);
        checkInvalidConstructor(1, -1);

        CapacityUnit capacityUnit = new CapacityUnit(10, 10);

        try {
            capacityUnit.setReadCapacityUnit(-1);
            fail();
        } catch (IllegalArgumentException e) {
        }
        try {
            capacityUnit.setWriteCapacityUnit(-1);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testEquals() {
        CapacityUnit c1 = new CapacityUnit(1, 10);
        CapacityUnit c2 = new CapacityUnit(1, 10);
        CapacityUnit c3 = new CapacityUnit(1, 11);

        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());
        assertTrue(!c2.equals(c3));
        assertTrue(c2.hashCode() != c3.hashCode());
        assertTrue(!c1.equals(c3));
        assertTrue(c1.hashCode() != c3.hashCode());

        // change not same to same
        c3.setWriteCapacityUnit(10);
        assertEquals(c1, c3);
        assertEquals(c1.hashCode(), c3.hashCode());

        // change same to not same
        c2.setReadCapacityUnit(0);
        assertTrue(!c1.equals(c2));
        assertTrue(c1.hashCode() != c2.hashCode());
    }


}
