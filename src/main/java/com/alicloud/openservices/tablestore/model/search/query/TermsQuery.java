package com.alicloud.openservices.tablestore.model.search.query;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 多个term查询。
 */
public class TermsQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_TermsQuery;

    private String fieldName;
    private List<ColumnValue> terms;
    private float weight = 1.0f;

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

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildTermsQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private List<ColumnValue> terms;
        private float weight = 1.0f;

        public Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder addTerm(Object term) {
            if (terms == null) {
                terms = new ArrayList<ColumnValue>();
            }
            this.terms.add(ValueUtil.toColumnValue(term));
            return this;
        }

        public Builder terms(Object... termArray) {
            if (terms == null) {
                terms = new ArrayList<ColumnValue>();
            }
            for (Object o : termArray) {
                terms.add(ValueUtil.toColumnValue(o));
            }
            return this;
        }

        @Override
        public TermsQuery build() {
            TermsQuery termsQuery = new TermsQuery();
            termsQuery.setFieldName(this.fieldName);
            termsQuery.setTerms(this.terms);
            termsQuery.setWeight(this.weight);
            return termsQuery;
        }
    }
}
