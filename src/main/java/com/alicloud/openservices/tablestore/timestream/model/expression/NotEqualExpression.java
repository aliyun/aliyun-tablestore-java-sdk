package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

public class NotEqualExpression implements Expression {
    private ColumnValue value;

    public NotEqualExpression(ColumnValue value){
        this.value = value;
    }

    public ColumnValue getValue() {
        return value;
    }

    @Override
    public Query getQuery(String columnName) {
        TermQuery notTermQuery = new TermQuery();
        notTermQuery.setFieldName(columnName);
        notTermQuery.setTerm(value);
        BoolQuery boolQuery = new BoolQuery();
        boolQuery.setMustNotQueries(Arrays.<Query>asList(notTermQuery));
        return boolQuery;
    }
}
