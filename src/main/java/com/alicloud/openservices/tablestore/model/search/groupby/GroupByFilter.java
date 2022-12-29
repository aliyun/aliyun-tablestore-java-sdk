package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 根据查询语句进行分组。
 */
public class GroupByFilter implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_FILTER;

    /**
     * GroupBy的名字，之后从GroupBy结果列表中根据该名字拿到GroupBy结果
     */
    private String groupByName;
    /**
     * filters
     */
    private List<Query> filters;

    /**
     * 子聚合
     */
    private List<Aggregation> subAggregations;

    /**
     * 子分组
     */
    private List<GroupBy> subGroupBys;

    public GroupByFilter() {
    }

    private GroupByFilter(Builder builder) {
        groupByName = builder.groupByName;
        filters = builder.filters;
        subAggregations = builder.subAggregations;
        subGroupBys = builder.subGroupBys;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    @Override
    public GroupByType getGroupByType() {
        return groupByType;
    }

    @Override
    public ByteString serialize() {
        return SearchGroupByBuilder.buildGroupByFilter(this).toByteString();
    }

    public List<Query> getFilters() {
        return filters;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByFilter setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public GroupByFilter setFilters(List<Query> filters) {
        this.filters = filters;
        return this;
    }

    public GroupByFilter setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public GroupByFilter setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private List<Query> filters;
        private List<Aggregation> subAggregations;
        private List<GroupBy> subGroupBys;

        private Builder() {}

        public Builder groupByName(String groupByName) {
            this.groupByName = groupByName;
            return this;
        }

        public Builder addFilter(QueryBuilder builder) {
            if (filters == null) {
                this.filters = new ArrayList<Query>();
            }
            this.filters.add(builder.build());
            return this;
        }

        public Builder addFilter(Query query) {
            if (filters == null) {
                this.filters = new ArrayList<Query>();
            }
            this.filters.add(query);
            return this;
        }

        public Builder addSubAggregation(AggregationBuilder builder) {
            if (subAggregations == null) {
                this.subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(builder.build());
            return this;
        }

        public Builder addSubAggregation(Aggregation aggregation) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(aggregation);
            return this;
        }

        public Builder addSubGroupBy(GroupByBuilder builder) {
            if (subGroupBys == null) {
                this.subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(builder.build());
            return this;
        }

        public Builder addSubGroupBy(GroupBy groupBy) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(groupBy);
            return this;
        }

        @Override
        public GroupByFilter build() {
            return new GroupByFilter(this);
        }
    }
}
