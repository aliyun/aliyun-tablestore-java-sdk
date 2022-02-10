package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.google.protobuf.ByteString;

/**
 * 根据地理位置坐标进行分组。
 */
public class GroupByGeoDistance implements GroupBy {

    private GroupByType groupByType = GroupByType.GROUP_BY_GEO_DISTANCE;

    /**
     * GroupBy的名字，之后从GroupBy结果列表中根据该名字拿到GroupBy结果
     */
    private String groupByName;
    /**
     * 字段名字
     */
    private String fieldName;

    /**
     * 设置起始中心点坐标
     */
    private GeoPoint origin;

    /**
     * 分组的依据范围
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

    public GroupByGeoDistance() {
    }

    private GroupByGeoDistance(Builder builder) {
        groupByName = builder.groupByName;
        fieldName = builder.fieldName;
        origin = builder.origin;
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
        return SearchGroupByBuilder.buildGroupByGeoDistance(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public GeoPoint getOrigin() {
        return origin;
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

    public GroupByGeoDistance setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public GroupByGeoDistance setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public GroupByGeoDistance setOrigin(GeoPoint origin) {
        this.origin = origin;
        return this;
    }

    public GroupByGeoDistance setRanges(List<Range> ranges) {
        this.ranges = ranges;
        return this;
    }

    public GroupByGeoDistance setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public GroupByGeoDistance setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private GeoPoint origin;
        private List<Range> ranges;
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

        /**
         * 设置起始中心点坐标
         *
         * @param lat 起始中心点坐标纬度
         * @param lon 起始中心点坐标经度
         * @return builder
         */
        public Builder origin(double lat, double lon) {
            origin = new GeoPoint(lat, lon);
            return this;
        }

        /**
         * 添加分组的范围
         *
         * @param from 起始值，单位是米。可以使用最小值 <pre> Double.MIN_VALUE</pre>
         * @param to   结束值，单位是米。可以使用最大值 <pre> Double.MAX_VALUE</pre>
         * @return 当前 builder
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
        public GroupByGeoDistance build() {
            return new GroupByGeoDistance(this);
        }
    }
}
