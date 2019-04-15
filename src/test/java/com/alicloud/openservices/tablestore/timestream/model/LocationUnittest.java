package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.timestream.model.filter.*;
import junit.framework.Assert;
import org.junit.Test;

public class LocationUnittest {
    @Test
    public void testBasic() {
        Location loc = new Location();
        double latitude = 123.1;
        double longitude = 234.2;
        loc.setLatitude(latitude).setLongitude(longitude);
        Assert.assertEquals(loc.getLatitude(), latitude);
        Assert.assertEquals(loc.getLongitude(), longitude);
        Assert.assertTrue(loc.toString().equals(latitude + "," + longitude));
    }

    @Test
    public void testParseFromStr() {
        Location loc = new Location("123.1,234.2");
        double latitude = 123.1;
        double longitude = 234.2;
        loc.setLatitude(latitude).setLongitude(longitude);
        Assert.assertEquals(loc.getLatitude(), latitude);
        Assert.assertEquals(loc.getLongitude(), longitude);
        Assert.assertTrue(loc.toString().equals(latitude + "," + longitude));
    }

    @Test
    public void testInvalidInput() {
        String locStr = "123.0";
        try {
            new Location(locStr);
            Assert.fail();
        } catch (Exception e) {
            // pass
        }
        locStr = ",123.0,";
        try {
            new Location(locStr);
            Assert.fail();
        } catch (Exception e) {
            // pass
        }
        locStr = "123.0,";
        try {
            new Location(locStr);
            Assert.fail();
        } catch (Exception e) {
            // pass
        }
        locStr = ",123.0";
        try {
            new Location(locStr);
            Assert.fail();
        } catch (Exception e) {
            // pass
        }
        locStr = "a,123.0";
        try {
            new Location(locStr);
            Assert.fail();
        } catch (Exception e) {
            // pass
        }
    }
}
