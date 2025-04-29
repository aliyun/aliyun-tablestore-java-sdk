package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * @Deprecated Please use {@link FunctionsScoreQuery} instead.
 * <p>A Query used to process document scores, which will perform a series of re-scoring operations on each matched document after the query is executed, and finally sort by the generated final score.</p>
 * <p>For examples, see {@link FieldValueFactor}</p>
 */
@Deprecated
public class FunctionScoreQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_FunctionScoreQuery;

    /**
     * Normal {@link Query}
     */
    private Query query;
    private FieldValueFactor fieldValueFactor;

    public FunctionScoreQuery(Query query, FieldValueFactor fieldValueFactor) {
        this.query = query;
        this.fieldValueFactor = fieldValueFactor;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public FieldValueFactor getFieldValueFactor() {
        return fieldValueFactor;
    }

    public void setFieldValueFactor(FieldValueFactor fieldValueFactor) {
        this.fieldValueFactor = fieldValueFactor;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildFunctionScoreQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder{
        private Query query;
        private FieldValueFactor fieldValueFactor;

        private Builder() {}

        public Builder query(QueryBuilder queryBuilder) {
            this.query = queryBuilder.build();
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder fieldValueFactor(String fieldName) {
            this.fieldValueFactor = new FieldValueFactor(fieldName);
            return this;
        }

        @Override
        public FunctionScoreQuery build() {
            return new FunctionScoreQuery(this.query, this.fieldValueFactor);
        }
    }
}
