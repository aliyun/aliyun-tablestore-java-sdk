package com.alicloud.openservices.tablestore.model.search.query;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Combined query (the most commonly used query under complex query conditions). The Bool query corresponds to BooleanQuery in Lucene, which consists of one or more clauses, and each clause has a specific type.
 * <ul>
 * <li>must: The document must fully match the condition</li>
 * <li>should: There will be one or more conditions under "should", and if at least one condition is met, the document satisfies "should"</li>
 * <li>must_not: The document must not match the condition</li>
 * </ul>
 */
public class BoolQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_BoolQuery;

    /**
     * The document must fully match all sub-queries.
     */
    private List<Query> mustQueries;
    /**
     * The document must not match any subquery
     */
    private List<Query> mustNotQueries;
    /**
     * The document must fully match all sub-filters
     */
    private List<Query> filterQueries;
    /**
     * The document should match at least one "should" condition, and those matching more will have higher scores.
     */
    private List<Query> shouldQueries;

    private Float weight;

    /**
     * Defines the minimum number of should clauses to be satisfied(deprecated).
     * <p>
     * This field is deprecated because the minimumShouldMatch parameter
     * can only accept integer and does not support percentages.
     * Use {@link #minShouldMatch} instead which stores the value as a String.
     *
     * @deprecated use {@link #minShouldMatch} instead
     */
    @Deprecated
    private Integer minimumShouldMatch;

    /**
     * Defines the minimum number of should clauses to be satisfied.
     * <p>
     * This field stores the minimumShouldMatch parameter as a String which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%").
     */
    private String minShouldMatch;

    public List<Query> getMustQueries() {
        return mustQueries;
    }

    public void setMustQueries(List<Query> mustQueries) {
        this.mustQueries = mustQueries;
    }

    public List<Query> getMustNotQueries() {
        return mustNotQueries;
    }

    public void setMustNotQueries(List<Query> mustNotQueries) {
        this.mustNotQueries = mustNotQueries;
    }

    public List<Query> getFilterQueries() {
        return filterQueries;
    }

    public void setFilterQueries(List<Query> filterQueries) {
        this.filterQueries = filterQueries;
    }

    public List<Query> getShouldQueries() {
        return shouldQueries;
    }

    public void setShouldQueries(List<Query> shouldQueries) {
        this.shouldQueries = shouldQueries;
    }

    /**
     * Get the weight of the query.
     * <p>
     * Weight is used to boost the relevance score of a query. In a BoolQuery,
     * the weight affects how much this query contributes to the final score of matching documents.
     * A higher weight value increases the contribution of this query to the overall document score.
     * The weight only takes effect on the must and should clauses, but not on filter and mustNot clauses.
     * The default weight value is 1.0f.
     * </p>
     *
     * @return the weight of the query
     */
    public Float getWeight() {
        return weight;
    }

    /**
     * Set the weight of the query.
     * <p>
     * Weight is used to boost the relevance score of a query. In a BoolQuery,
     * the weight affects how much this query contributes to the final score of matching documents.
     * A higher weight value increases the contribution of this query to the overall document score.
     * The weight only takes effect on the must and should clauses, but not on filter and mustNot clauses.
     * The default weight value is 1.0f.
     * </p>
     *
     * @param weight the weight of the query
     */
    public void setWeight(Float weight) {
        this.weight = weight;
    }

    /**
     * Get the minimum number of matches required as an Integer.
     * <p>
     * This method is deprecated because the minimumShouldMatch parameter
     * can only accept integer values and does not support percentage (e.g., "50%").
     * </p>
     *
     * @return the minimum number of matches as an Integer, or null if not set
     * @throws IllegalStateException if the stored value is not a valid integer
     * @deprecated use {@link #getMinShouldMatch()} instead which returns the value as a String
     */
    @Deprecated
    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    /**
     * Set the minimum number of matches required as an Integer.
     *
     * @param minimumShouldMatch the minimum number of matches as an Integer
     * @deprecated use {@link #setMinShouldMatch(String)} or {@link #setMinShouldMatch(int)} instead
     */
    @Deprecated
    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    /**
     * Get the minimum number of matches required as a String.
     * <p>
     * This method returns the raw string value of minimumShouldMatch which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%"). This provides
     * more flexibility than {@link #getMinimumShouldMatch()} which only supports integers.
     * </p>
     * <p>
     * In a later version, this method will be renamed to getMinimumShouldMatch().
     * </p>
     *
     * @return the minimum number of matches as a String, or null if not set
     */
    public String getMinShouldMatch() {
        return minShouldMatch;
    }

    /**
     * Set the minimum number of matches required as a String.
     * <p>
     * This method accepts the minimumShouldMatch parameter as a String which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%").
     * </p>
     *
     * @param minShouldMatch the minimum number of matches as a String
     */
    public void setMinShouldMatch(String minShouldMatch) {
        this.minShouldMatch = minShouldMatch;
    }

    public void setMinShouldMatch(int minShouldMatch) {
        this.minShouldMatch = String.valueOf(minShouldMatch);
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildBoolQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private List<Query> mustQueries;
        private List<Query> mustNotQueries;
        private List<Query> filterQueries;
        private List<Query> shouldQueries;
        @Deprecated
        private Integer minimumShouldMatch;
        private String minShouldMatch;
        private Float weight;

        public Builder() {}

        public Builder must(QueryBuilder queryBuilder) {
            if (this.mustQueries == null) {
                this.mustQueries = new ArrayList<Query>();
            }
            this.mustQueries.add(queryBuilder.build());
            return this;
        }

        public Builder must(Query query) {
            if (this.mustQueries == null) {
                this.mustQueries = new ArrayList<Query>();
            }
            this.mustQueries.add(query);
            return this;
        }

        public Builder mustNot(QueryBuilder queryBuilder) {
            if (this.mustNotQueries == null) {
                this.mustNotQueries = new ArrayList<Query>();
            }
            this.mustNotQueries.add(queryBuilder.build());
            return this;
        }

        public Builder mustNot(Query query) {
            if (this.mustNotQueries == null) {
                this.mustNotQueries = new ArrayList<Query>();
            }
            this.mustNotQueries.add(query);
            return this;
        }

        public Builder filter(QueryBuilder queryBuilder) {
            if (this.filterQueries == null) {
                this.filterQueries = new ArrayList<Query>();
            }
            this.filterQueries.add(queryBuilder.build());
            return this;
        }

        public Builder filter(Query query) {
            if (this.filterQueries == null) {
                this.filterQueries = new ArrayList<Query>();
            }
            this.filterQueries.add(query);
            return this;
        }

        public Builder should(QueryBuilder queryBuilder) {
            if (this.shouldQueries == null) {
                this.shouldQueries = new ArrayList<Query>();
            }
            this.shouldQueries.add(queryBuilder.build());
            return this;
        }

        public Builder should(Query query) {
            if (this.shouldQueries == null) {
                this.shouldQueries = new ArrayList<Query>();
            }
            this.shouldQueries.add(query);
            return this;
        }

        public Builder weight(Float weight) {
            this.weight = weight;
            return this;
        }

        @Deprecated
        public Builder minimumShouldMatch(int value) {
            this.minimumShouldMatch = value;
            return this;
        }

        public Builder minShouldMatch(String value) {
            this.minShouldMatch = value;
            return this;
        }

        public Builder minShouldMatch(int value) {
            this.minShouldMatch = String.valueOf(value);
            return this;
        }

        @Override
        public BoolQuery build() {
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setMustQueries(this.mustQueries);
            boolQuery.setMustNotQueries(this.mustNotQueries);
            boolQuery.setFilterQueries(this.filterQueries);
            boolQuery.setShouldQueries(this.shouldQueries);
            boolQuery.setMinimumShouldMatch(this.minimumShouldMatch);
            boolQuery.setMinShouldMatch(this.minShouldMatch);
            boolQuery.setWeight(this.weight);
            return boolQuery;
        }
    }
}
