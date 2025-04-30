package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;


/**
 * K-nearest neighbor query
 */
public class KnnVectorQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_KnnVectorQuery;

    /**
     * Field name
     */
    private String fieldName;

    /**
     * Query the topK nearest values
     */
    private Integer topK;

    /**
     * When the index field type is float32, use this field for querying.
     */
    private float[] float32QueryVector;


    /**
     * Query filter
     */
    private Query filter;

    private Float weight;

    /**
     * <p>Controls the minimum score threshold for vectors returned by the vector query (current sub-query). Optional, default value is 0 (indicating no vectors will be filtered), and the value must be greater than or equal to 0.</p>
     * <p>If this parameter is set, the result set returned by the query will not include documents where the distance score calculated with the query vector {@link KnnVectorQuery#float32QueryVector} is less than {@link KnnVectorQuery#minScore};</p>
     * <p>The vector distance scoring algorithm is detailed in the "Distance Metric Algorithm Description" section of the official documentation.</p>
     * <p>Note: The documents in the result set are sorted based on the overall query score, not solely on the score from the {@link KnnVectorQuery} sub-query. If you need to sort the results purely by the distance to the query vector {@link KnnVectorQuery#float32QueryVector}, place the scalar query within the {@link KnnVectorQuery#filter}.</p>
     */
    private Float minScore;

    /**
     * <p>Controls the amplification of vector queries. Optional, the value range is [topK, maxTopK].</p>
     * <p>The larger the value of numCandidates, the more data the engine accesses during the query, which increases the recall rate of the returned results, but may also increase the query time.</p>
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
