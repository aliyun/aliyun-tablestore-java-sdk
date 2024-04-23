package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by yanglian on 2019/4/10.
 */
public class WildcardExpressionUnitTest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        WildcardExpression expression = new WildcardExpression(value);
        Assert.assertEquals(value, expression.getValue());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof WildcardQuery);
        WildcardQuery termQuery = (WildcardQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), colName);
        Assert.assertEquals(termQuery.getValue(), value);
    }
}
