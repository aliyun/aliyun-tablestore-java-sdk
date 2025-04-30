package com.alicloud.openservices.tablestore.model.search;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.filter.SearchFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;

/**
 * Build the SearchQuery, this entity class will be passed to the server through SearchRequest, telling the server our search parameters.
 */
public class SearchQuery {

    /**
     * Returns the total number of matching rows
     */
    public static final int TRACK_TOTAL_COUNT = Integer.MAX_VALUE;

    /**
     * Does not return the count of matching rows
     */
    public static final int TRACK_TOTAL_COUNT_DISABLED = -1;

    /**
     * Pagination start quantity
     */
    private Integer offset;

    /**
     * Page size, i.e., the number of rows returned
     */
    private Integer limit;

    /**
     * Query statement
     */
    private Query query;
    /**
     * Query highlight configuration
     */
    private Highlight highlight;
    /**
     * Field collapsing
     * Can achieve deduplication of results for a certain field.
     */
    private Collapse collapse;

    /**
     * Sorting
     * <p>Set the sorting method for the results. This parameter supports sorting by multiple fields.</p>
     */
    private Sort sort;

    /**
     * Customizable expected maximum number of documents to hit, the smaller this value is, the better the performance will be.
     * <li>{@link SearchQuery#TRACK_TOTAL_COUNT} returns the total number of rows matched setGetTotalCount(true)</li>
     * <li>{@link SearchQuery#TRACK_TOTAL_COUNT_DISABLED} does not return the count information of matched rows setGetTotalCount(false)</li>
     */
    private int trackTotalCount = TRACK_TOTAL_COUNT_DISABLED;

    /**
     * Filter
     * Filters the query results of the Query statement
     */
    private SearchFilter filter;

    private List<Aggregation> aggregationList;

    private List<GroupBy> groupByList;

    private byte[] token;

    public static Builder newBuilder() {
        return new Builder();
    }

    public List<Aggregation> getAggregationList() {
        return aggregationList;
    }


    /**
     * Set the agg parameter in the statistical aggregation.
     * @param aggregationList Built using {@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}
     */
    public void setAggregationList(
        List<Aggregation> aggregationList) {
        this.aggregationList = aggregationList;
    }

    public List<GroupBy> getGroupByList() {
        return groupByList;
    }

    /**
     * Set the groupby parameter in the statistical aggregation.
     *
     * @param groupByList Built using {@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}
     */
    public void setGroupByList(List<GroupBy> groupByList) {
        this.groupByList = groupByList;
    }

    public Integer getOffset() {
        return offset;
    }

    public SearchQuery setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public SearchQuery setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public SearchQuery setQuery(Query query) {
        this.query = query;
        return this;
    }

    public Highlight getHighlight() {
        return highlight;
    }

    public SearchQuery setHighlight(Highlight highlight) {
        this.highlight = highlight;
        return this;
    }

    public Collapse getCollapse() {
        return collapse;
    }

    public SearchQuery setCollapse(Collapse collapse) {
        this.collapse = collapse;
        return this;
    }

    public Sort getSort() {
        return sort;
    }

    public SearchQuery setSort(Sort sort) {
        this.sort = sort;
        return this;
    }

    /**
     * It is recommended to use {@link SearchQuery#getTrackTotalCount}
     */
    @Deprecated
    public boolean isGetTotalCount() {
        return this.trackTotalCount == TRACK_TOTAL_COUNT;
    }

    /**
     * It is recommended to use {@link SearchQuery#setTrackTotalCount}
     */
    @Deprecated
    public void setGetTotalCount(boolean getTotalCount) {
        if (getTotalCount) {
            this.trackTotalCount = TRACK_TOTAL_COUNT;
        } else {
            this.trackTotalCount = TRACK_TOTAL_COUNT_DISABLED;
        }
    }

    public int getTrackTotalCount() {
        return trackTotalCount;
    }

    public void setTrackTotalCount(int trackTotalCount) {
        this.trackTotalCount = trackTotalCount;
    }

    public byte[] getToken() {
        return token;
    }

    public void setToken(byte[] token) {
        this.token = token;
        // The Sort condition is encoded in the Token, so there is no need to set Sort when setting Token.
        if (null != token) {
            this.sort = null;
        }
    }

    public SearchFilter getFilter() {
        return filter;
    }

    public SearchQuery setFilter(SearchFilter filter) {
        this.filter = filter;
        return this;
    }

    public SearchQuery() {
    }

    public SearchQuery toCopy() {
        SearchQuery copy = new SearchQuery();
        copy.setAggregationList(this.getAggregationList());
        copy.setCollapse(this.getCollapse());
        copy.setTrackTotalCount(this.getTrackTotalCount());
        copy.setGroupByList(this.getGroupByList());
        copy.setLimit(this.getLimit());
        copy.setOffset(this.getOffset());
        copy.setQuery(this.getQuery());
        copy.setHighlight(this.getHighlight());
        copy.setSort(this.getSort());
        copy.setToken(this.getToken());
        copy.setFilter(this.getFilter());
        return copy;
    }

    private SearchQuery(Builder builder) {
        setOffset(builder.offset);
        setLimit(builder.limit);
        setQuery(builder.query);
        setHighlight(builder.highlight);
        setCollapse(builder.collapse);
        setSort(builder.sort);
        setTrackTotalCount(builder.trackTotalCount);
        setAggregationList(builder.aggregationList);
        setGroupByList(builder.groupByList);
        setToken(builder.token);
        setFilter(builder.filter);
    }

    public static final class Builder {
        private Integer offset;
        private Integer limit;
        private Query query;
        private Highlight highlight;
        private Collapse collapse;
        private Sort sort;
        private int trackTotalCount = TRACK_TOTAL_COUNT_DISABLED;
        private List<Aggregation> aggregationList;
        private List<GroupBy> groupByList;
        private byte[] token;
        private SearchFilter filter;

        private Builder() {}

        public Builder offset(int val) {
            offset = val;
            return this;
        }

        public Builder limit(int val) {
            limit = val;
            return this;
        }

        public Builder query(QueryBuilder queryBuilder) {
            query = queryBuilder.build();
            return this;
        }

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder highlight(Highlight highlight) {
            this.highlight = highlight;
            return this;
        }

        public Builder highlight(Highlight.Builder highlightBuilder) {
            this.highlight = highlightBuilder.build();
            return this;
        }

        /**
         * Field collapsing
         * Can achieve deduplication of results for a specific field.
         */
        public Builder collapse(String fieldName) {
            collapse = new Collapse(fieldName);
            return this;
        }

        public Builder sort(Sort val) {
            sort = val;
            return this;
        }

        /**
         * Whether to return the total number of rows matched
         */
        public Builder getTotalCount(boolean val) {
            if(val) {
                trackTotalCount = TRACK_TOTAL_COUNT;
            } else {
                trackTotalCount = TRACK_TOTAL_COUNT_DISABLED;
            }
            return this;
        }

        public Builder trackTotalCount(int val) {
            trackTotalCount = val;
            return this;
        }

        /**
         * Add an Agg in the statistical aggregation
         * @param aggregationBuilder Build using {@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}
         */
        public Builder addAggregation(AggregationBuilder aggregationBuilder) {
            if (aggregationList == null) {
                aggregationList = new ArrayList<Aggregation>();
            }
            aggregationList.add(aggregationBuilder.build());
            return this;
        }

        /**
         * Add an Agg in the statistical aggregation
         * @param aggregation Built using {@link com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders}
         */
        public Builder addAggregation(Aggregation aggregation) {
            if (aggregationList == null) {
                aggregationList = new ArrayList<Aggregation>();
            }
            aggregationList.add(aggregation);
            return this;
        }

        /**
         * Add a GroupBy to the statistical aggregation
         * @param groupByBuilder Built using {@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}
         */
        public Builder addGroupBy(GroupByBuilder groupByBuilder) {
            if (groupByList == null) {
                groupByList = new ArrayList<GroupBy>();
            }
            groupByList.add(groupByBuilder.build());
            return this;
        }

        /**
         * Add a GroupBy to the statistical aggregation
         * @param groupBy Build using {@link com.alicloud.openservices.tablestore.model.search.groupby.GroupByBuilders}
         */
        public Builder addGroupBy(GroupBy groupBy) {
            if (groupByList == null) {
                groupByList = new ArrayList<GroupBy>();
            }
            groupByList.add(groupBy);
            return this;
        }

        /**
         * Parameter for pagination
         */
        public Builder token(byte[] val) {
            token = val;
            return this;
        }

        /**
         * Filter
         * Filters the query results of the Query statement
         */
        public Builder filter(SearchFilter filter) {
            this.filter = filter;
            return this;
        }

        public SearchQuery build() {
            return new SearchQuery(this);
        }
    }
}
