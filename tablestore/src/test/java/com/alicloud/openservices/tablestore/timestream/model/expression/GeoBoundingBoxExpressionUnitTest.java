package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/4/10.
 */
public class GeoBoundingBoxExpressionUnitTest {
    @Test
    public void testBasic() {
        String left = "123,345";
        String right = "234,456";
        GeoBoundingBoxExpression expression = new GeoBoundingBoxExpression(left, right);
        String colName = "loc123";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof GeoBoundingBoxQuery);
        GeoBoundingBoxQuery boundingBoxQuery = (GeoBoundingBoxQuery)query;

        Assert.assertEquals(boundingBoxQuery.getFieldName(), colName);
        Assert.assertEquals(boundingBoxQuery.getTopLeft(), left);
        Assert.assertEquals(boundingBoxQuery.getBottomRight(), right);
    }
}
