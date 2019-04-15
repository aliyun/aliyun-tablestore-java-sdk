package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class RangeExpressionUnittest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        TimeRange timeRange = TimeRange.range(0, 1000, TimeUnit.SECONDS);
        RangeExpression expression = new RangeExpression(
                ColumnValue.fromLong(timeRange.getBeginTime()),
                ColumnValue.fromLong(timeRange.getEndTime()));
        Assert.assertEquals(timeRange.getBeginTime(), expression.getBegin().asLong());
        Assert.assertEquals(timeRange.getEndTime(), expression.getEnd().asLong());
        String colName = "tag";
        Query query = expression.getQuery(colName);
        Assert.assertTrue(query instanceof RangeQuery);
        RangeQuery termQuery = (RangeQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), colName);
        Assert.assertEquals(termQuery.getFrom().asLong(), timeRange.getBeginTime());
        Assert.assertTrue(termQuery.isIncludeLower());
        Assert.assertEquals(termQuery.getTo().asLong(), timeRange.getEndTime());
        Assert.assertFalse(termQuery.isIncludeUpper());
    }
}
