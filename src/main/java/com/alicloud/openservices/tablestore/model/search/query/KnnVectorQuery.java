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

    /**
     * <p>控制向量查询（当前子查询）返回向量的最小分数门限，选填，默认值为0（表示不过滤任何向量），取值范围大于等于0。</p>
     * <p>若设置此项，查询返回的结果集中将不会出现与查询向量 {@link KnnVectorQuery#float32QueryVector} 计算距离得分小于 {@link KnnVectorQuery#minScore} 的文档；</p>
     * <p>向量距离得分算法参考官方文档中的“距离度量算法说明”</p>
     * <p>注意：结果集中各个文档是依据查询整体得分排序的，而非单纯依据 {@link KnnVectorQuery} 子 Query 得分排序。如果您需要仅按照与查询向量 {@link KnnVectorQuery#float32QueryVector} 的距离排序，请将标量查询设置在 {@link KnnVectorQuery#filter} 中</p>
     */
    private Float minScore;

    /**
     * <p>控制向量查询放大，选填，取值范围为[topK, maxTopK]。</p>
     * <p>numCandidates 的值越大，引擎查询时访问的数据越多，返回结果的召回率也就越高，但是查询耗时可能会变长</p>
     */
    private Integer numCandidates;

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

    public Float getMinScore() {
        return minScore;
    }

    public void setMinScore(Float minScore) {
        this.minScore = minScore;
    }

    public Integer getNumCandidates() {
        return numCandidates;
    }

    public void setNumCandidates(Integer numCandidates) {
        this.numCandidates = numCandidates;
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

        public Builder minScore(Float minScore) {
            knnVectorQuery.setMinScore(minScore);
            return this;
        }

        public Builder numCandidates(Integer numCandidates) {
            knnVectorQuery.setNumCandidates(numCandidates);
            return this;
        }

        public KnnVectorQuery build() {
            return knnVectorQuery;
        }
    }
}
