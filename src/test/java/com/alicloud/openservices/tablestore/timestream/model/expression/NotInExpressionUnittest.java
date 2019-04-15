package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import org.junit.Assert;
import org.junit.Test;

public class NotInExpressionUnittest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        String key = "key_" + now;
        int count = 10;
        ColumnValue[] valueList = new ColumnValue[count];
        for (int i = 0; i < count; ++i) {
            valueList[i] =  ColumnValue.fromString("value_" + now + "_" + i);
        }
        NotInExpression expression = new NotInExpression(valueList);
        Assert.assertEquals(valueList, expression.getValueList());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof BoolQuery);
        Assert.assertEquals(((BoolQuery)query).getMustNotQueries().size(), 10);
        for (int i = 0; i < count; ++i) {
            Assert.assertTrue(((BoolQuery) query).getMustNotQueries().get(i) instanceof TermQuery);
            TermQuery termQuery = (TermQuery) ((BoolQuery) query).getMustNotQueries().get(i);
            Assert.assertEquals(termQuery.getFieldName(), colName);
            Assert.assertEquals(termQuery.getTerm().asString(), valueList[i].asString());
        }
    }
}
