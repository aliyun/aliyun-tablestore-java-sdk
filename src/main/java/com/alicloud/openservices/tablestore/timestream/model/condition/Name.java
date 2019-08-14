package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.expression.*;

/**
 * {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier#name}过滤器，查询满足条件的TimestreamMeta
 */
public class Name implements Condition {
    private Expression expression;

    private Name() {}

    public static Name equal(String value) {
        Name condition = new Name();
        condition.expression = new EqualExpression(ColumnValue.fromString(value));
        return condition;
    }

    public static Name notEqual(String value) {
        Name condition = new Name();
        condition.expression = new NotEqualExpression(ColumnValue.fromString(value));
        return condition;
    }

    public static Name in(String[] valueList) {
        Name condition = new Name();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        condition.expression = new InExpression(columnValues);
        return condition;
    }

    public static Name notIn(String[] valueList) {
        Name condition = new Name();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        condition.expression = new NotInExpression(columnValues);
        return condition;
    }

    /**
     * 前缀匹配
     * @param prefix
     * @return
     */
    public static Name prefix(String prefix) {
        Name condition = new Name();
        condition.expression = new PrefixExpression(prefix);
        return condition;
    }

    public Query getQuery() {
        return this.expression.getQuery(TableMetaGenerator.CN_PK1);
    }
}

