package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yanglian on 2019/4/10.
 */
public class GeoPolygonExpressionUnittest {
    @Test
    public void testBasic1() {
        List<String> points = new ArrayList<String>();
        points.add("123,123");
        points.add("234,234");
        GeoPolygonExpression expression = new GeoPolygonExpression(points);
        String colName = "loc1234";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof GeoPolygonQuery);
        GeoPolygonQuery polygonQuery = (GeoPolygonQuery)query;
        Assert.assertEquals(polygonQuery.getFieldName(), colName);
        Assert.assertEquals(polygonQuery.getPoints(), points);
    }

    @Test
    public void testBasic2() {
        String[] points = new String[2];
        points[0] = "123,123";
        points[1] = "234,234";
        GeoPolygonExpression expression = new GeoPolygonExpression(points);
        String colName = "loc1234";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof GeoPolygonQuery);
        GeoPolygonQuery polygonQuery = (GeoPolygonQuery)query;
        Assert.assertEquals(polygonQuery.getFieldName(), colName);
        Assert.assertEquals(polygonQuery.getPoints().size(), 2);
        Assert.assertEquals(polygonQuery.getPoints().get(0), points[0]);
        Assert.assertEquals(polygonQuery.getPoints().get(1), points[1]);
    }
}
