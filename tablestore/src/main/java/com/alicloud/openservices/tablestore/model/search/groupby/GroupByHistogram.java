package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Perform histogram statistics on a specific field.
 * <p>Example:</p>
 * <P>The data consists of 1, 1, 5, 5, 8, 10. Performing histogram statistics on this dataset with an interval of 5 would return range statistics like: "0->2; 5->3; 10->1".</P>
 */
public class GroupByHistogram implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_HISTOGRAM;

    /**
     * The name of the GroupBy, which is used to retrieve the GroupBy result from the GroupBy result list later.
     */
    private String groupByName;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * Interval
     */
    private ColumnValue interval;
    /**
     * Default value for missing fields.
     * <p>If a document is missing this field, what default value should be used</p>
     */
    private ColumnValue missing;
    /**
     * Group deviation
     */
    private ColumnValue offset;
    /**
     * Sorting
     */
    private List<GroupBySorter> groupBySorters;
    /**
     * Minimum number of documents
     */
    private Long minDocCount;
    /**
     * Bucket boundary limit
     */
    private FieldRange fieldRange;
    /**
     * Sub-aggregation
     */
    private List<Aggregation> subAggregations;

    /**
     * Sub-group
     */
    private List<GroupBy> subGroupBys;

    public GroupByHistogram() {
    }

    private GroupByHistogram(Builder builder) {
        groupByName = builder.groupByName;
        fieldName = builder.fieldName;
        interval = builder.interval;
        groupBySorters = builder.groupBySorters;
        missing = builder.missing;
        offset = builder.offset;
        minDocCount = builder.minDocCount;
        fieldRange = builder.fieldRange;
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
        return SearchGroupByBuilder.buildGroupByHistogram(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public Long getMinDocCount() {
        return minDocCount;
    }

    public ColumnValue getInterval() {
        return interval;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public ColumnValue getOffset() {
        return offset;
    }

    public List<GroupBySorter> getGroupBySorters() {
        return groupBySorters;
    }

    public FieldRange getFieldRange() {
        return fieldRange;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByHistogram setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public GroupByHistogram setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public GroupByHistogram setMinDocCount(Long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public GroupByHistogram setGroupBySorters(List<GroupBySorter> groupBySorters) {
        this.groupBySorters = groupBySorters;
        return this;
    }

    public GroupByHistogram setInterval(ColumnValue interval) {
        this.interval = interval;
        return this;
    }

    public GroupByHistogram setMissing(ColumnValue missing) {
        this.missing = missing;
        return this;
    }

    public GroupByHistogram setOffset(ColumnValue offset) {
        this.offset = offset;
        return this;
    }

    public GroupByHistogram setFieldRange(FieldRange fieldRange) {
        this.fieldRange = fieldRange;
        return this;
    }

    public GroupByHistogram setSubAggregations(
        List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public GroupByHistogram setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private ColumnValue interval;
        private ColumnValue missing;
        private ColumnValue offset;
        private Long minDocCount;
        private List<GroupBySorter> groupBySorters;
        private FieldRange fieldRange;
        private List<Aggregation> subAggregations;
        private List<GroupBy> subGroupBys;

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

        public Builder interval(Object interval) {
            this.interval = ValueUtil.toColumnValue(interval);
            return this;
        }

        /**
         * Default value for missing fields.
         * <p>If a document is missing this field, what default value should be used</p>
         */
        public Builder missing(Object missing) {
            this.missing = ValueUtil.toColumnValue(missing);
            return this;
        }

        public Builder offset(Object offset) {
            this.offset = ValueUtil.toColumnValue(offset);
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

        public Builder addFieldRange(Object min, Object max) {
            if (fieldRange == null) {
                fieldRange = new FieldRange(ValueUtil.toColumnValue(min), ValueUtil.toColumnValue(max));
            }
            return this;
        }

        public Builder addSubAggregation(AggregationBuilder builder) {
            if (subAggregations == null) {
                subAggregations = new ArrayList<Aggregation>();
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
                subGroupBys = new ArrayList<GroupBy>();
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
        public GroupByHistogram build() {
            return new GroupByHistogram(this);
        }
    }
}
