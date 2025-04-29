package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestTimeRange {

    @Test
    public void testConstructor() {
        TimeRange tr = new TimeRange();
        assertEquals(tr.getStart(), 0);
        assertEquals(tr.getEnd(), Long.MAX_VALUE);

        tr = new TimeRange(1912);
        assertEquals(tr.getStart(), 1912);
        assertEquals(tr.getEnd(), Long.MAX_VALUE);

        tr = new TimeRange(1912, 1999);
        assertEquals(tr.getStart(), 1912);
        assertEquals(tr.getEnd(), 1999);
    }

    @Test
    public void testWithIn() {
        TimeRange tr = new TimeRange(1912, 1999);
        assertTrue(tr.withinTimeRange(1912));
        assertTrue(tr.withinTimeRange(1916));
        assertTrue(!tr.withinTimeRange(1999));
        assertTrue(!tr.withinTimeRange(2000));
    }

    @Test
    public void testCompare() {
        TimeRange tr = new TimeRange(1912, 1999);
        assertEquals(tr.compare(1912), 0);
        assertEquals(tr.compare(1913), 0);
        assertEquals(tr.compare(1911), -1);
        assertEquals(tr.compare(1999), 1);
        assertEquals(tr.compare(2000), 1);
    }

    @Test
    public void testEquals() {
        TimeRange tr1 = new TimeRange(1912, 1999);
        TimeRange tr2 = new TimeRange(1912, 1999);
        assertEquals(tr1, tr2);
        TimeRange tr3 = new TimeRange(1912, 2000);
        assertTrue(!tr1.equals(tr3));
    }

    @Test
    public void testInvalidArguments() {
        try {
            new TimeRange(-1);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new TimeRange(0, 0);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new TimeRange(1, 0);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
