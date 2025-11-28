package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * DisMaxQuery is used to select documents that match at least one of the specified sub-queries.
 * The final score of a document is calculated based on the highest score among all sub-queries,
 * plus a "tie-breaker" factor multiplied by the scores of other matching sub-queries.
 */
public class DisMaxQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_DisMaxQuery;

    /**
     * A list of sub-queries to be executed.
     */
    private List<Query> queries;

    /**
     * The tie-breaker coefficient, which controls how much of the scores from lower-scoring
     * sub-queries contribute to the final score.
     * Value should be between 0.0 and 1.0.
     */
    private Float tieBreaker;

    /**
     * An optional weight to apply to the entire query.
     */
    private Float weight;

    /**
     * Gets the list of sub-queries.
     *
     * @return the list of sub-queries
     */
    public List<Query> getQueries() {
        return queries;
    }

    /**
     * Sets the list of sub-queries.
     *
     * @param queries the list of sub-queries to set
     */
    public void setQueries(List<Query> queries) {
        this.queries = queries;
    }

    /**
     * Gets the tie-breaker coefficient.
     *
     * @return the tie-breaker coefficient
     */
    public Float getTieBreaker() {
        return tieBreaker;
    }

    /**
     * Sets the tie-breaker coefficient.
     *
     * @param tieBreaker the tie-breaker coefficient to set (between 0.0 and 1.0)
     */
    public void setTieBreaker(Float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    /**
     * Gets the weight applied to the entire query.
     *
     * @return the weight
     */
    public Float getWeight() {
        return weight;
    }

    /**
     * Sets the weight applied to the entire query.
     *
     * @param weight the weight to set
     */
    public void setWeight(Float weight) {
        this.weight = weight;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildDisMaxQuery(this).toByteString();
    }

    /**
     * Creates a new builder for constructing a DisMaxQuery instance.
     *
     * @return a new Builder instance
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder class for constructing DisMaxQuery instances.
     */
    public static final class Builder {
        private Float weight;
        private Float tieBreaker;
        private List<Query> queries;

        private Builder() {}

        /**
         * Sets the weight for the DisMaxQuery.
         *
         * @param weight the weight to set
         * @return this builder instance
         */
        public Builder weight(Float weight) {
            this.weight = weight;
            return this;
        }

        /**
         * Sets the tie-breaker coefficient for the DisMaxQuery.
         *
         * @param tieBreaker the tie-breaker coefficient to set (between 0.0 and 1.0)
         * @return this builder instance
         */
        public Builder tieBreaker(Float tieBreaker) {
            this.tieBreaker = tieBreaker;
            return this;
        }

        /**
         * Adds a sub-query to the DisMaxQuery.
         *
         * @param query the sub-query to add
         * @return this builder instance
         */
        public Builder addQuery(Query query) {
            if (this.queries == null) {
                this.queries = new ArrayList<>();
            }
            this.queries.add(query);
            return this;
        }

        /**
         * Adds a sub-query built from a QueryBuilder to the DisMaxQuery.
         *
         * @param queryBuilder the QueryBuilder to build and add as a sub-query
         * @return this builder instance
         */
        public Builder addQuery(QueryBuilder queryBuilder) {
            return addQuery(queryBuilder.build());
        }

        /**
         * Builds and returns a new DisMaxQuery instance with the configured parameters.
         *
         * @return a new DisMaxQuery instance
         */
        public DisMaxQuery build() {
            DisMaxQuery disMaxQuery = new DisMaxQuery();
            disMaxQuery.setWeight(weight);
            disMaxQuery.setTieBreaker(tieBreaker);
            disMaxQuery.setQueries(queries);
            return disMaxQuery;
        }
    }
}
