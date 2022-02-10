package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.SearchAggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.google.protobuf.ByteString;

/**
 * Return the first few rows in the group.
 */
public class TopRowsAggregation implements Aggregation {

    private AggregationType aggregationType = AggregationType.AGG_TOP_ROWS;

    private String aggName;
    private Integer limit;
    private Sort sort;

    public TopRowsAggregation() {
    }

    private TopRowsAggregation(Builder builder) {
        setAggName(builder.aggName);
        setLimit(builder.limit);
        setSort(builder.sort);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return aggregationType;
    }

    @Override
    public ByteString serialize() {
        return SearchAggregationBuilder.buildTopRowsAggregation(this).toByteString();
    }

    public TopRowsAggregation setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public TopRowsAggregation setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Sort getSort() {
        return sort;
    }

    public TopRowsAggregation setSort(Sort sort) {
        this.sort = sort;
        return this;
    }

    public static final class Builder implements AggregationBuilder {
        private String aggName;
        private Integer limit;
        private Sort sort;

        public Builder() {}

        public Builder aggName(String aggName) {
            this.aggName = aggName;
            return this;
        }

        public Builder limit(Integer limit) {
            this.limit = limit;
            return this;
        }

        public Builder sort(Sort sort) {
            this.sort = sort;
            return this;
        }

        public TopRowsAggregation build() {
            return new TopRowsAggregation(this);
        }
    }
}
