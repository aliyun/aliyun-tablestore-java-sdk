package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;

import static com.alicloud.openservices.tablestore.timestream.internal.Utils.buildTagValue;

public class PrefixExpression implements Expression {
    private String value;

    public PrefixExpression(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public Query getQuery(String columnName) {
        PrefixQuery prefixQuery = new PrefixQuery();
        prefixQuery.setFieldName(columnName);
        prefixQuery.setPrefix(value);
        return prefixQuery;
    }
}
