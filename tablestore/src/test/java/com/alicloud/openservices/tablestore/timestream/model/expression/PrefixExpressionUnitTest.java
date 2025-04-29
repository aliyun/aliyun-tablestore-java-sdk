package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import org.junit.Assert;
import org.junit.Test;

public class PrefixExpressionUnitTest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        PrefixExpression expression = new PrefixExpression(value);
        Assert.assertEquals(value, expression.getValue());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof PrefixQuery);
        PrefixQuery termQuery = (PrefixQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), colName);
        Assert.assertEquals(termQuery.getPrefix(), value);
    }
}
