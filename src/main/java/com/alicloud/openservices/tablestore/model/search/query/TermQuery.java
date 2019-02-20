package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 精确的term查询。
 */
public class TermQuery implements Query {

    private String fieldName;
    private ColumnValue term;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public ColumnValue getTerm() {
        return term;
    }

    public void setTerm(ColumnValue term) {
        this.term = term;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_TermQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildTermQuery(this).toByteString();
    }
}
