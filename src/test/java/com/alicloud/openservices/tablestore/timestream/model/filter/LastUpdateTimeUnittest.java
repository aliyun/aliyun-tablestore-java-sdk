package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class LastUpdateTimeUnittest {
    @Test
    public void testBasic() {
        long now = System.currentTimeMillis();
        TimeRange timeRange = TimeRange.range(0, 1000, TimeUnit.SECONDS);
        LastUpdateTime updateTime = LastUpdateTime.in(timeRange);
        Query query = updateTime.getQuery();
        Assert.assertTrue(query instanceof RangeQuery);
        RangeQuery termQuery = (RangeQuery)query;
        Assert.assertEquals(termQuery.getFieldName(), TableMetaGenerator.CN_TAMESTAMP_NAME);
        Assert.assertEquals(termQuery.getFrom().asLong(), timeRange.getBeginTime());
        Assert.assertTrue(termQuery.isIncludeLower());
        Assert.assertEquals(termQuery.getTo().asLong(), timeRange.getEndTime());
        Assert.assertFalse(termQuery.isIncludeUpper());
    }
}
