package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermsQuery;
import org.junit.Assert;
import org.junit.Test;

public class InExpressionUnitTest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        int count = 10;
        ColumnValue[] valueList = new ColumnValue[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  ColumnValue.fromString("value_" + now + "_" + i);
        }
        InExpression expression = new InExpression(valueList);
        Assert.assertEquals(valueList, expression.getValueList());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof TermsQuery);
        Assert.assertEquals(((TermsQuery)query).getFieldName(), colName);
        Assert.assertEquals(((TermsQuery)query).getTerms().size(), 10);
        for (int i = 0; i < count; ++i) {
            Assert.assertEquals(((TermsQuery)query).getTerms().get(i).asString(), valueList[i].asString());
        }
    }
}
