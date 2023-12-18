package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 用于处理文档分值的Query，它会在查询结束后对每一个匹配的文档进行一系列的重打分操作，最后以生成的最终分数进行排序。
 * <p>举例见{@link FieldValueFactor}</p>
 */
public class FunctionScoreQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_FunctionScoreQuery;

    /**
     * 正常的{@link Query}
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
