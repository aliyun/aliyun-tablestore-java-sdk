package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;
import com.alicloud.openservices.tablestore.timestream.model.expression.Expression;
import com.alicloud.openservices.tablestore.timestream.model.expression.RangeExpression;

/**
 * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamMeta#updateTime}过滤器，查询满足条件的TimestreamMeta
 */
public class LastUpdateTime implements Condition {
    private Expression expression;

    private LastUpdateTime() {}

    public static LastUpdateTime in(TimeRange timeRange) {
        LastUpdateTime condition = new LastUpdateTime();
        condition.expression = new RangeExpression(
                ColumnValue.fromLong(timeRange.getBeginTime()),
                ColumnValue.fromLong(timeRange.getEndTime()));
        return condition;
    }

    public Query getQuery() {
        return  this.expression.getQuery(TableMetaGenerator.CN_TAMESTAMP_NAME);
    }
}
