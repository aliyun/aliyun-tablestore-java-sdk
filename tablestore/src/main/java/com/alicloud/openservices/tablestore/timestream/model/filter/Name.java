package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.expression.*;

/**
 * Filter for {@link com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier#name}, queries TimestreamMeta that meets the condition.
 */
public class Name implements Filter {
    private Expression expression;

    private Name() {}

    public static Name equal(String value) {
        Name filter = new Name();
        filter.expression = new EqualExpression(ColumnValue.fromString(value));
        return filter;
    }

    public static Name notEqual(String value) {
        Name filter = new Name();
        filter.expression = new NotEqualExpression(ColumnValue.fromString(value));
        return filter;
    }

    public static Name in(String[] valueList) {
        Name filter = new Name();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        filter.expression = new InExpression(columnValues);
        return filter;
    }

    public static Name notIn(String[] valueList) {
        Name filter = new Name();
        ColumnValue[] columnValues = new ColumnValue[valueList.length];
        for (int i = 0; i < valueList.length; ++i) {
            columnValues[i] = ColumnValue.fromString(valueList[i]);
        }
        filter.expression = new NotInExpression(columnValues);
        return filter;
    }

    /**
     * Prefix match
     * @param prefix
     * @return
     */
    public static Name prefix(String prefix) {
        Name filter = new Name();
        filter.expression = new PrefixExpression(prefix);
        return filter;
    }

    public Query getQuery() {
        return this.expression.getQuery(TableMetaGenerator.CN_PK1);
    }
}

