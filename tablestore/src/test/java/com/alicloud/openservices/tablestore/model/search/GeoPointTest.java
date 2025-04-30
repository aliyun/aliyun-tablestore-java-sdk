package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class GeoPointTest extends BaseSearchTest {
    @Test
    public void testGetLat() {
        GeoPoint geoPoint = new GeoPoint();
        double expectedLat = 1.0;
        geoPoint.setLat(expectedLat);
        assertEquals(expectedLat, geoPoint.getLat(), 0.0001);
    }

    @Test
    public void testGetLon() {
        GeoPoint geoPoint = new GeoPoint();
        double expectedLon = 1.0;
        geoPoint.setLon(expectedLon);
        assertEquals(expectedLon, geoPoint.getLon(), 0.0001);
    }

    @Test
    public void testJsonize() {
        GeoPoint geoPoint = new GeoPoint(1.0, 2.0);
        //language=JSON
        String expectedJson = "{\n" +
                "  \"lon\": 2.0,\n" +
                "  \"lat\": 1.0\n" +
                "}";
        String actualJson = geoPoint.jsonize();
        assertEquals(expectedJson, actualJson);
    }
}
