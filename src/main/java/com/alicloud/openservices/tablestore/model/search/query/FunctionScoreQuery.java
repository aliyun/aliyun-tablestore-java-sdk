package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 用于处理文档分值的Query，它会在查询结束后对每一个匹配的文档进行一系列的重打分操作，最后以生成的最终分数进行排序。
 * <p>举例见{@link FieldValueFactor}</p>
 */
public class FunctionScoreQuery implements Query {

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
        return QueryType.QueryType_FunctionScoreQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildFunctionScoreQuery(this).toByteString();
    }
}
