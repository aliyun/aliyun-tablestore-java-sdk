package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestReservedThroughput {

    @Test
    public void testConstructor() {
        ReservedThroughput rt = new ReservedThroughput(new CapacityUnit(10, 0));
        assertEquals(rt.getCapacityUnit().getReadCapacityUnit(), 10);
        assertEquals(rt.getCapacityUnit().getWriteCapacityUnit(), 0);
    }

    @Test
    public void testEquals() {
        ReservedThroughput rt1 = new ReservedThroughput(new CapacityUnit(10, 11));
        ReservedThroughput rt2 = new ReservedThroughput(new CapacityUnit(10, 11));
        ReservedThroughput rt3 = new ReservedThroughput(new CapacityUnit(12, 11));
        assertEquals(rt1, rt2);
        assertEquals(rt1.hashCode(), rt2.hashCode());

        assertTrue(!rt1.equals(rt3));
        assertTrue(rt1.hashCode() != rt3.hashCode());
    }
}
