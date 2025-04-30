package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeoGridTest extends BaseSearchTest {
    @Test
    public void testConstructor() {
        GeoPoint topLeft = new GeoPoint(1, 2);
        GeoPoint bottomRight = new GeoPoint(3, 4);
        GeoGrid geoGrid = new GeoGrid(topLeft, bottomRight);
        assertEquals(topLeft, geoGrid.getTopLeft());
        assertEquals(bottomRight, geoGrid.getBottomRight());
    }

    @Test
    public void testGetTopLeft() {
        GeoPoint topLeft = new GeoPoint(10, 20);
        GeoGrid geoGrid = new GeoGrid();
        geoGrid.setTopLeft(topLeft);
        GeoPoint result = geoGrid.getTopLeft();
        assertEquals(topLeft, result);
    }

    @Test
    public void testGetBottomRight() {
        GeoPoint bottomRight = new GeoPoint(10, 20);
        GeoGrid geoGrid = new GeoGrid();
        geoGrid.setBottomRight(bottomRight);
        GeoPoint result = geoGrid.getBottomRight();
        assertEquals(bottomRight, result);
    }

    @Test
    public void testJsonize() {
        GeoPoint topLeft = new GeoPoint(1, 2);
        GeoPoint bottomRight = new GeoPoint(3, 4);
        GeoGrid geoGrid = new GeoGrid(topLeft, bottomRight);
        //language=JSON
        String expected = "{\n" +
                "  \"topLeft\": {\n" +
                "    \"lon\": 2.0,\n" +
                "    \"lat\": 1.0\n" +
                "  },\n" +
                "  \"bottomRight\": {\n" +
                "    \"lon\": 4.0,\n" +
                "    \"lat\": 3.0\n" +
                "  }\n" +
                "}";
        String actual = geoGrid.jsonize();
        assertEquals(expected, actual);
    }

    @Test
    public void testJsonizeWithNullTopLeft() {
        GeoPoint bottomRight = new GeoPoint(3, 4);
        GeoGrid geoGrid = new GeoGrid(null, bottomRight);
        //language=JSON
        String expected = "{\n" +
                "  \"topLeft\": null,\n" +
                "  \"bottomRight\": {\n" +
                "    \"lon\": 4.0,\n" +
                "    \"lat\": 3.0\n" +
                "  }\n" +
                "}";
        String actual = geoGrid.jsonize();
        assertEquals(expected, actual);
    }

    @Test
    public void testJsonizeWithNullBottomRight() {
        GeoPoint topLeft = new GeoPoint(1, 2);
        GeoGrid geoGrid = new GeoGrid(topLeft, null);
        //language=JSON
        String expected = "{\n" +
                "  \"topLeft\": {\n" +
                "    \"lon\": 2.0,\n" +
                "    \"lat\": 1.0\n" +
                "  },\n" +
                "  \"bottomRight\": null\n" +
                "}";
        String actual = geoGrid.jsonize();
        assertEquals(expected, actual);
    }

    @Test
    public void testJsonizeWithNullTopLeftAndBottomRight() {
        GeoGrid geoGrid = new GeoGrid(null, null);
        //language=JSON
        String expected = "{\n" +
                "  \"topLeft\": null,\n" +
                "  \"bottomRight\": null\n" +
                "}";
        String actual = geoGrid.jsonize();
        assertEquals(expected, actual);
    }
}
