package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.google.protobuf.ByteString;

/**
 * 根据范围进行分组。
 */
public class GroupByRange implements GroupBy {

    private GroupByType groupByType = GroupByType.GROUP_BY_RANGE;

    /**
     * GroupBy的名字，之后从GroupBy结果列表中根据该名字拿到GroupBy结果
     */
    private String groupByName;
    /**
     * 字段名字
     */
    private String fieldName;

    /**
     * range list.
     * note: includes the from value and excludes the to value for each range.
     */
    private List<Range> ranges;

    /**
     * 子聚合
     */
    private List<Aggregation> subAggregations;

    /**
     * 子分组
     */
    private List<GroupBy> subGroupBys;

    public GroupByRange() {
    }

    private GroupByRange(Builder builder) {
        groupByName = builder.groupByName;
        fieldName = builder.fieldName;
        ranges = builder.ranges;
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
        return SearchGroupByBuilder.buildGroupByRange(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<Range> getRanges() {
        return ranges;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByRange setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public GroupByRange setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public GroupByRange setRanges(List<Range> ranges) {
        this.ranges = ranges;
        return this;
    }

    public GroupByRange setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public GroupByRange setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private List<Range> ranges;
        private List<Aggregation> subAggregations;
        private List<GroupBy> subGroupBys;

        private Builder() {}

        public Builder groupByName(String groupByName) {
            this.groupByName = groupByName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        /**
         * add one Range.
         * note: includes the from value and excludes the to value for each range.
         *
         * @param from you can use <pre> Double.MIN_VALUE</pre>
         * @param to   you can use <pre> Double.MAX_VALUE</pre>
         */
        public Builder addRange(double from, double to) {
            if (ranges == null) {
                ranges = new ArrayList<Range>();
            }
            this.ranges.add(new Range(from, to));
            return this;
        }

        public Builder addSubAggregation(AggregationBuilder builder) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(builder.build());
            return this;
        }

        public Builder addSubGroupBy(GroupByBuilder builder) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(builder.build());
            return this;
        }

        @Override
        public GroupByRange build() {
            return new GroupByRange(this);
        }
    }
}
