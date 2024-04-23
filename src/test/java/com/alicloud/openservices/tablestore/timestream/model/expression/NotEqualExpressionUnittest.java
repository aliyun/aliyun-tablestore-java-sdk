package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class NotEqualExpressionUnitTest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        String value = "value_" + now;
        NotEqualExpression expression = new NotEqualExpression(ColumnValue.fromString(value));
        Assert.assertEquals(value, expression.getValue().asString());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 1);
        Assert.assertTrue(((BoolQuery)query).getMustNotQueries().get(0) instanceof TermQuery);
        TermQuery termQuery = (TermQuery)((BoolQuery)query).getMustNotQueries().get(0);
        Assert.assertEquals(termQuery.getFieldName(), colName);
        Assert.assertEquals(termQuery.getTerm().asString(), value);
    }
}
