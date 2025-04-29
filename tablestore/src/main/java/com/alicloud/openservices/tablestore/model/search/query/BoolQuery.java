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
    /**
     * Defines the minimum number of should clauses to be satisfied.
     */
    private Integer minimumShouldMatch;

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

    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(int minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
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
        private Integer minimumShouldMatch;

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

        public Builder minimumShouldMatch(int value) {
            this.minimumShouldMatch = value;
            return this;
        }

        @Override
        public BoolQuery build() {
            BoolQuery boolQuery = new BoolQuery();
            boolQuery.setMustQueries(this.mustQueries);
            boolQuery.setMustNotQueries(this.mustNotQueries);
            boolQuery.setFilterQueries(this.filterQueries);
            boolQuery.setShouldQueries(this.shouldQueries);
            boolQuery.minimumShouldMatch = this.minimumShouldMatch;
            return boolQuery;
        }
    }
}
