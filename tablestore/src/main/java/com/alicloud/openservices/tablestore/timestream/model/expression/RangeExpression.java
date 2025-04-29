package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.TimeRange;

import java.util.Arrays;

public class RangeExpression implements Expression {
    private ColumnValue begin;
    private ColumnValue end;

    public RangeExpression(ColumnValue begin, ColumnValue end){
        this.begin = begin;
        this.end = end;
    }

    public ColumnValue getBegin() {
        return begin;
    }

    public ColumnValue getEnd() {
        return end;
    }

    @Override
    public Query getQuery(String columnName) {
        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName(columnName);
        rangeQuery.setFrom(begin, true);
        rangeQuery.setTo(end, false);
        return rangeQuery;
    }
}
