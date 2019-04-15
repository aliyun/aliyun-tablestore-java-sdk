package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/4/10.
 */
public class GeoDistanceExpressionUnittest {
    @Test
    public void testBasic() {
        String pos = "123,345";
        double distance = 2.0;
        GeoDistanceExpression expression = new GeoDistanceExpression(pos, distance);
        String colName = "loc123";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof GeoDistanceQuery);
        GeoDistanceQuery distanceQuery = (GeoDistanceQuery)query;

        Assert.assertEquals(distanceQuery.getFieldName(), colName);
        Assert.assertEquals(distanceQuery.getCenterPoint(), pos);
        Assert.assertTrue(distanceQuery.getDistanceInMeter() == distance);
    }
}
