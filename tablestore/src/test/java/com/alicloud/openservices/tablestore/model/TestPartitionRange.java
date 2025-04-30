package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPartitionRange {

    @Test
    public void testConstructor() {
        PartitionRange range = new PartitionRange(PrimaryKeyValue.fromString("A"),
                PrimaryKeyValue.fromString("B"));
        assertEquals(range.getBegin(), PrimaryKeyValue.fromString("A"));
        assertEquals(range.getEnd(), PrimaryKeyValue.fromString("B"));

        // test type not same
        try {
            new PartitionRange(PrimaryKeyValue.fromLong(10), PrimaryKeyValue.fromString("A"));
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
