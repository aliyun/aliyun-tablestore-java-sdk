package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.ByteString;

/**
 * 精确的term查询。
 */
public class TermQuery implements Query {

    private QueryType queryType = QueryType.QueryType_TermQuery;

    private String fieldName;
    private ColumnValue term;
    private float weight = 1.0f;

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
        return SearchQueryBuilder.buildTermQuery(this).toByteString();
    }

    public TermQuery() {
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private ColumnValue term;
        private float weight = 1.0f;

        public Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        private Builder() {}

        public Builder field(String val) {
            fieldName = val;
            return this;
        }

        public Builder term(Object val) {
            term = ValueUtil.toColumnValue(val);
            return this;
        }

        @Override
        public TermQuery build() {
            TermQuery termQuery = new TermQuery();
            termQuery.setTerm(this.term);
            termQuery.setWeight(this.weight);
            termQuery.setFieldName(this.fieldName);
            return termQuery;
        }
    }
}
