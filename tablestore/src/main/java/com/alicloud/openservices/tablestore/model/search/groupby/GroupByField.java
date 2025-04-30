package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Perform grouping statistics on a specific field.
 * <p>Example:</p>
 * <P>In inventory bills, there are "basketballs", "footballs", "badminton shuttlecocks", etc. Aggregate this field and return information such as: "Basketball: 10", "Football: 5", "Tennis: 1" etc.</P>
 */
public class GroupByField implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_FIELD;

    /**
     * The name of the GroupBy, which is used to retrieve the GroupBy result from the GroupBy result list later.
     */
    private String groupByName;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * Returns the number of groups
     */
    private Integer size;

    /**
     * Sorting
     */
    private List<GroupBySorter> groupBySorters;

    /**
     * Sub-aggregation
     */
    private List<Aggregation> subAggregations;

    /**
     * Sub-group
     */
    private List<GroupBy> subGroupBys;

    /**
     * Minimum number of documents
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

        public Builder addSubAggregation(Aggregation aggregation) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
            }
            this.subAggregations.add(aggregation);
            return this;
        }

        public Builder addSubGroupBy(GroupByBuilder groupByBuilder) {
            if (subGroupBys == null) {
                subGroupBys = new ArrayList<GroupBy>();
            }
            this.subGroupBys.add(groupByBuilder.build());
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
        public GroupByField build() {
            return new GroupByField(this);
        }
    }
}
