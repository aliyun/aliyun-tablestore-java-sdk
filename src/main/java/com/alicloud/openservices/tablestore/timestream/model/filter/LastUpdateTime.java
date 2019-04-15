package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;
import com.alicloud.openservices.tablestore.timestream.model.expression.Expression;
import com.alicloud.openservices.tablestore.timestream.model.expression.RangeExpression;

import java.util.concurrent.TimeUnit;

/**
 * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta#updateTime}过滤器，查询满足条件的TimestreamMeta
 */
public class LastUpdateTime implements Filter {
    private Expression expression;

    private LastUpdateTime() {}

    public static LastUpdateTime in(TimeRange timeRange) {
        LastUpdateTime filter = new LastUpdateTime();
        filter.expression = new RangeExpression(
                ColumnValue.fromLong(timeRange.getBeginTime()),
                ColumnValue.fromLong(timeRange.getEndTime()));
        return filter;
    }

    public Query getQuery() {
        return  this.expression.getQuery(TableMetaGenerator.CN_TAMESTAMP_NAME);
    }
}
