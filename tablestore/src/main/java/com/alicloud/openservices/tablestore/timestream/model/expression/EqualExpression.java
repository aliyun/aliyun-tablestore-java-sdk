package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

public class EqualExpression implements Expression {
    private ColumnValue value;

    public EqualExpression(ColumnValue value){
        this.value = value;
    }

    public ColumnValue getValue() {
        return value;
    }

    @Override
    public Query getQuery(String columnName) {
        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName(columnName);
        termQuery.setTerm(value);
        return termQuery;
    }
}
