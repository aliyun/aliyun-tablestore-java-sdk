package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.google.protobuf.ByteString;

/**
 * 对某一个字段进行分组统计。
 * <p>举例：</p>
 * <P>库存账单里有“篮球”、“足球”、“羽毛球”等，对这一个字段进行聚合，返回： “篮球：10个”，“足球：5个”，“网球：1个”这样的聚合信息。</P>
 */
public class GroupByField implements GroupBy {

    private GroupByType groupByType = GroupByType.GROUP_BY_FIELD;

    /**
     * GroupBy的名字，之后从GroupBy结果列表中根据该名字拿到GroupBy结果
     */
    private String groupByName;
    /**
     * 字段名字
     */
    private String fieldName;
    /**
     * 返回多少个分组
     */
    private Integer size;

    /**
     * 排序
     */
    private List<GroupBySorter> groupBySorters;

    /**
     * 子聚合
     */
    private List<Aggregation> subAggregations;

    /**
     * 子分组
     */
    private List<GroupBy> subGroupBys;

    /**
     * 最小文档数
     */
    private Long minDocCount;

    public GroupByField() {
    }

    private GroupByField(Builder builder) {
        groupByName = builder.groupByName;
        fieldName = builder.fieldName;
        size = builder.size;
        groupBySorters = builder.groupBySorters;
        subAggregations = builder.subAggregations;
        subGroupBys = builder.subGroupBys;
        minDocCount = builder.minDocCount;
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
        return SearchGroupByBuilder.buildGroupByField(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public Long getMinDocCount() {
        return minDocCount;
    }

    public Integer getSize() {
        return size;
    }

    public List<GroupBySorter> getGroupBySorters() {
        return groupBySorters;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByField setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public GroupByField setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public GroupByField setMinDocCount(Long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public GroupByField setSize(Integer size) {
        this.size = size;
        return this;
    }

    public GroupByField setGroupBySorters(List<GroupBySorter> groupBySorters) {
        this.groupBySorters = groupBySorters;
        return this;
    }

    public GroupByField setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public GroupByField setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private Integer size;
        private List<GroupBySorter> groupBySorters;
        private List<Aggregation> subAggregations;
        private List<GroupBy> subGroupBys;
        private Long minDocCount;

        private Builder() {
        }

        public Builder groupByName(String groupByName) {
            this.groupByName = groupByName;
            return this;
        }

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder size(Integer size) {
            this.size = size;
            return this;
        }

        public Builder minDocCount(Long minDocCount) {
            this.minDocCount = minDocCount;
            return this;
        }

        public Builder addGroupBySorter(GroupBySorter... groupBySorter) {
            if (groupBySorters == null) {
                groupBySorters = new ArrayList<GroupBySorter>();
            }
            this.groupBySorters.addAll(Arrays.asList(groupBySorter));
            return this;
        }

        public Builder addGroupBySorter(List<GroupBySorter> groupBySorter) {
            if (groupBySorters == null) {
                groupBySorters = new ArrayList<GroupBySorter>();
            }
            this.groupBySorters.addAll(groupBySorter);
            return this;
        }

        public Builder addSubAggregation(AggregationBuilder aggregationBuilder) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(aggregationBuilder.build());
            return this;
        }

        public Builder addSubGroupBy(GroupByBuilder groupByBuilder) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(groupByBuilder.build());
            return this;
        }

        @Override
        public GroupByField build() {
            return new GroupByField(this);
        }
    }
}
