package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;


/**
 * K最邻近查询
 */
public class KnnVectorQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_KnnVectorQuery;

    /**
     * 字段名
     */
    private String fieldName;

    /**
     * 查询最邻近的topK个值
     */
    private Integer topK;

    /**
     * 当索引字段类型是 float32 时候，使用该字段进行查询。
     */
    private float[] float32QueryVector;

    /**
     * 查询过滤器
     */
    private Query filter;

    private Float weight;

    public KnnVectorQuery() {
    }

    public String getFieldName() {
        return fieldName;
    }

    public KnnVectorQuery setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public Integer getTopK() {
        return topK;
    }

    public KnnVectorQuery setTopK(Integer topK) {
        this.topK = topK;
        return this;
    }

    public float[] getFloat32QueryVector() {
        return float32QueryVector;
    }

    public KnnVectorQuery setFloat32QueryVector(float[] float32QueryVector) {
        this.float32QueryVector = float32QueryVector;
        return this;
    }
    public Query getFilter() {
        return filter;
    }

    public KnnVectorQuery setFilter(Query queryFilter) {
        this.filter = queryFilter;
        return this;
    }

    public KnnVectorQuery setFilter(QueryBuilder queryFilter) {
        this.filter = queryFilter.build();
        return this;
    }

    public Float getWeight() {
        return weight;
    }

    public KnnVectorQuery setWeight(Float weight) {
        this.weight = weight;
        return this;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildKnnVectorQuery(this).toByteString();
    }

    protected static KnnVectorQuery.Builder newBuilder() {
        return new KnnVectorQuery.Builder();
    }


    public static final class Builder implements QueryBuilder {

        private final KnnVectorQuery knnVectorQuery;

        private Builder() {
            knnVectorQuery = new KnnVectorQuery();
        }

        public Builder field(String fieldName) {
            knnVectorQuery.setFieldName(fieldName);
            return this;
        }

        public Builder topK(Integer topK) {
            knnVectorQuery.setTopK(topK);
            return this;
        }

        public Builder queryVector(float[] float32QueryVector) {
            knnVectorQuery.setFloat32QueryVector(float32QueryVector);
            return this;
        }
        public Builder filter(Query filter) {
            knnVectorQuery.setFilter(filter);
            return this;
        }

        public Builder weight(Float weight) {
            knnVectorQuery.setWeight(weight);
            return this;
        }

        public KnnVectorQuery build() {
            return knnVectorQuery;
        }
    }
}
