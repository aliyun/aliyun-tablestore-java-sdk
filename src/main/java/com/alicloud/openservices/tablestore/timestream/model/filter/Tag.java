package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.expression.*;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

/**
 * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier#tags}过滤器，查询满足条件的TimestreamMeta
 */
public class Tag implements Filter {
    private Expression expression;

    private Tag() {}

    public static Tag equal(String key, String value) {
        Tag filter = new Tag();
        filter.expression = new EqualExpression(
                ColumnValue.fromString(buildTagValue(key, value)));
        return filter;
    }

    public static Tag notEqual(String key, String value) {
        Tag filter = new Tag();
        filter.expression = new NotEqualExpression(
                ColumnValue.fromString(buildTagValue(key, value)));
        return filter;
    }

    public static Tag in(String key, String[] valueList) {
        Tag filter = new Tag();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(buildTagValue(key, valueList[i]));
        }
        filter.expression = new InExpression(columnValues);
        return filter;
    }

    public static Tag notIn(String key, String[] valueList) {
        Tag filter = new Tag();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(buildTagValue(key, valueList[i]));
        }
        filter.expression = new NotInExpression(columnValues);
        return filter;
    }

    /**
     * 前缀匹配
     * @param key
     * @param value
     * @return
     */
    public static Tag prefix(String key, String value) {
        Tag filter = new Tag();
        filter.expression = new PrefixExpression(buildTagValue(key, value));
        return filter;
    }

    public Query getQuery() {
        return  this.expression.getQuery(TableMetaGenerator.CN_PK2);
    }
}
