package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.google.gson.Gson;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestGeoDecode extends BaseSearchTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestGeoDecode.class);

    @Test
    public void testGeoJsonDecode() {
        // SQL Sample: SELECT xxxx FROM tableName WHERE location = Condition
        Gson gson = new Gson();
        // GeoDistanceQuery SQL Condition
        String s = "{\"centerPoint\": \"104.05358919674954, 30.530045901643962\", \"distanceInMeter\": 3000.0 }";
        GeoDistanceQuery distanceQuery = gson.fromJson(s, GeoDistanceQuery.class);
        assertEquals("104.05358919674954, 30.530045901643962", distanceQuery.getCenterPoint());
        assertEquals(3000.0, distanceQuery.getDistanceInMeter(), 0);
        assertNull(distanceQuery.getFieldName());

        // GeoBoundingBoxQuery SQL Condition
        s = "{\"topLeft\": \"120.1595116589601, 30.257664116603074\", \"bottomRight\": \"120.15958497923747, 30.256593333442616\"}";
        GeoBoundingBoxQuery boundingBoxQuery = gson.fromJson(s, GeoBoundingBoxQuery.class);
        assertEquals("120.1595116589601, 30.257664116603074", boundingBoxQuery.getTopLeft());
        assertEquals("120.15958497923747, 30.256593333442616", boundingBoxQuery.getBottomRight());

        // GeoPolygonQuery SQL Condition
        s = "{\"points\": [\"120.1595116589601, 30.257664116603074\", \"120.15958497923747, 30.256593333442616\", " +
                "\"120.15960002901203, 30.25654136528343\", \"120.15966611840484, 30.257671402958163\", " +
                "\"120.1595116589601, 30.257664116603074\", \"120.1595116589601, 30.257664116603074\"]}";
        GeoPolygonQuery polygonQuery = gson.fromJson(s, GeoPolygonQuery.class);
        assertEquals(6, polygonQuery.getPoints().size());
        assertEquals("120.1595116589601, 30.257664116603074", polygonQuery.getPoints().get(0));
        assertEquals("120.15958497923747, 30.256593333442616", polygonQuery.getPoints().get(1));
        assertEquals("120.15960002901203, 30.25654136528343", polygonQuery.getPoints().get(2));
        assertEquals("120.15966611840484, 30.257671402958163", polygonQuery.getPoints().get(3));
        assertEquals("120.1595116589601, 30.257664116603074", polygonQuery.getPoints().get(4));
        assertEquals("120.1595116589601, 30.257664116603074", polygonQuery.getPoints().get(5));
    }

    @Test
    public void testDefault() {
        GeoDistanceQuery query = new GeoDistanceQuery();
        LOGGER.info(query.getCenterPoint());
        LOGGER.info(String.valueOf(query.getDistanceInMeter()));
    }

}
