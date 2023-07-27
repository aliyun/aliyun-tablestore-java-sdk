package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupByDateHistogram implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_DATE_HISTOGRAM;

    /**
     * GroupBy的名字，之后从GroupBy结果列表中根据该名字拿到GroupBy结果
     */
    private String groupByName;
    /**
     * 字段名字
     */
    private String fieldName;
    /**
     * 时间间隔
     */
    private DateTimeValue interval;
    /**
     * 桶边界限制
     */
    private FieldRange fieldRange;


    /**
     * 缺失字段的默认值。
     * <p>如果一个文档缺少该字段，则采用什么默认值</p>
     */
    private ColumnValue missing;
    /**
     * 最小文档数
     */
    private Long minDocCount;
    /**
     * 时区
     */
    private String timeZone;
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

    public GroupByDateHistogram() {
    }

    private GroupByDateHistogram(Builder builder) {
        groupByName = builder.groupByName;
        fieldName = builder.fieldName;
        interval = builder.interval;
        fieldRange = builder.fieldRange;
        groupBySorters = builder.groupBySorters;
        missing = builder.missing;
        minDocCount = builder.minDocCount;
        timeZone = builder.timeZone;
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
        return SearchGroupByBuilder.buildGroupByDateHistogram(this).toByteString();
    }

    public GroupByDateHistogram setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public GroupByDateHistogram setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public DateTimeValue getInterval() {
        return interval;
    }

    public GroupByDateHistogram setInterval(DateTimeValue interval) {
        this.interval = interval;
        return this;
    }

    public GroupByDateHistogram setInterval(Integer value, DateTimeUnit unit) {
        this.interval = new DateTimeValue(value, unit);
        return this;
    }

    public ColumnValue getMissing() {
        return missing;
    }

    public GroupByDateHistogram setMissing(ColumnValue missing) {
        this.missing = missing;
        return this;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public GroupByDateHistogram setTimeZone(String timeZone) {
        this.timeZone = timeZone;
        return this;
    }

    public List<GroupBySorter> getGroupBySorters() {
        return groupBySorters;
    }

    public GroupByDateHistogram setGroupBySorters(List<GroupBySorter> groupBySorters) {
        this.groupBySorters = groupBySorters;
        return this;
    }

    public Long getMinDocCount() {
        return minDocCount;
    }

    public GroupByDateHistogram setMinDocCount(long minDocCount) {
        this.minDocCount = minDocCount;
        return this;
    }

    public FieldRange getFieldRange() {
        return fieldRange;
    }

    public GroupByDateHistogram setFieldRange(FieldRange fieldRange) {
        this.fieldRange = fieldRange;
        return this;
    }

    public GroupByDateHistogram setFieldRange(String min, String max) {
        this.fieldRange = new FieldRange(ColumnValue.fromString(min), ColumnValue.fromString(max));
        return this;
    }

    public GroupByDateHistogram setFieldRange(long min, long max) {
        this.fieldRange = new FieldRange(ColumnValue.fromLong(min), ColumnValue.fromLong(max));
        return this;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public GroupByDateHistogram setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByDateHistogram setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private DateTimeValue interval;
        private FieldRange fieldRange;
        private ColumnValue missing;
        private String timeZone;
        private Long minDocCount;
        private List<GroupBySorter> groupBySorters;
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

        public Builder interval(DateTimeValue interval) {
            this.interval = interval;
            return this;
        }

        public Builder interval(Integer value, DateTimeUnit unit) {
            this.interval = new DateTimeValue(value, unit);
            return this;
        }

        public Builder fieldRange(FieldRange fieldRange) {
            this.fieldRange = fieldRange;
            return this;
        }

        public Builder fieldRange(long min, long max) {
            this.fieldRange = new FieldRange(ColumnValue.fromLong(min), ColumnValue.fromLong(max));
            return this;
        }

        public Builder fieldRange(String min, String max) {
            this.fieldRange = new FieldRange(ColumnValue.fromString(min), ColumnValue.fromString(max));
            return this;
        }

        public Builder timeZone(String timeZone) {
            this.timeZone = timeZone;
            return this;
        }

        /**
         * 缺失字段的默认值。
         * <p>如果一个文档缺少该字段，则采用什么默认值</p>
         */
        public Builder missing(Object missing) {
            this.missing = ValueUtil.toColumnValue(missing);
            return this;
        }

        public Builder minDocCount(long minDocCount) {
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
        public GroupByDateHistogram build() {
            return new GroupByDateHistogram(this);
        }
    }
}
