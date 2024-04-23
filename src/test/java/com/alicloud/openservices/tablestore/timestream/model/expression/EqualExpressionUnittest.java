package com.alicloud.openservices.tablestore.timestream.model.expression;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import org.junit.Assert;
import org.junit.Test;

public class EqualExpressionUnitTest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String value = "value_" + now;
        EqualExpression expression = new EqualExpression(ColumnValue.fromString(value));
        Assert.assertEquals(value, expression.getValue().asString());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof TermQuery);
        TermQuery termQuery = (TermQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), colName);
        Assert.assertEquals(termQuery.getTerm().asString(), value);
    }
}
