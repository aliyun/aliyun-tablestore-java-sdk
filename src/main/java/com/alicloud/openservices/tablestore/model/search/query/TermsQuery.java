package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * 多个term查询。
 */
public class TermsQuery implements Query {

    private String fieldName;
    private List<ColumnValue> terms;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<ColumnValue> getTerms() {
        return terms;
    }

    public void setTerms(List<ColumnValue> terms) {
        this.terms = terms;
    }

    public void addTerm(ColumnValue term) {
        if (this.terms == null) {
            this.terms = new ArrayList<ColumnValue>();
        }
        this.terms.add(term);
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_TermsQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildTermsQuery(this).toByteString();
    }

}
