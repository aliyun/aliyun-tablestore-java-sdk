package com.alicloud.openservices.tablestore.model.search.query;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 联合查询（复杂查询条件下用的最多的一个查询）。Bool查询对应Lucene中的BooleanQuery，它由一个或者多个子句组成，每个子句都有特定的类型。
 * <ul>
 * <li>must: 文档必须完全匹配条件</li>
 * <li>should: should下面会带一个以上的条件，至少满足一个条件，这个文档就符合should</li>
 * <li>must_not: 文档必须不匹配条件</li>
 * </ul>
 */
public class BoolQuery implements Query {

    private QueryType queryType = QueryType.QueryType_BoolQuery;

    /**
     * 文档必须完全匹配所有的子query
     */
    private List<Query> mustQueries;
    /**
     * 文档必须不能匹配任何子query
     */
    private List<Query> mustNotQueries;
    /**
     * 文档必须完全匹配所有的子filter
     */
    private List<Query> filterQueries;
    /**
     * 文档应该至少匹配一个should，匹配多的得分会高
     */
    private List<Query> shouldQueries;
    /**
     * 定义了至少满足几个should子句。
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

        public Builder mustNot(QueryBuilder queryBuilder) {
            if (this.mustNotQueries == null) {
                this.mustNotQueries = new ArrayList<Query>();
            }
            this.mustNotQueries.add(queryBuilder.build());
            return this;
        }

        public Builder filter(QueryBuilder queryBuilder) {
            if (this.filterQueries == null) {
                this.filterQueries = new ArrayList<Query>();
            }
            this.filterQueries.add(queryBuilder.build());
            return this;
        }

        public Builder should(QueryBuilder queryBuilder) {
            if (this.shouldQueries == null) {
                this.shouldQueries = new ArrayList<Query>();
            }
            this.shouldQueries.add(queryBuilder.build());
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
