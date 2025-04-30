package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Group by geographical location coordinates.
 */
public class GroupByGeoDistance implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_GEO_DISTANCE;

    /**
     * The name of the GroupBy, which is used to retrieve the GroupBy result from the GroupBy result list later.
     */
    private String groupByName;
    /**
     * Field name
     */
    private String fieldName;

    /**
     * Set the starting center point coordinates
     */
    private GeoPoint origin;

    /**
     * The basis for grouping ranges
     */
    private List<Range> ranges;

    /**
     * Sub-aggregation
     */
    private List<Aggregation> subAggregations;

    /**
     * Sub-group
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
         * Set the starting center point coordinates
         *
         * @param lat Latitude of the starting center point coordinates
         * @param lon Longitude of the starting center point coordinates
         * @return builder
         */
        public Builder origin(double lat, double lon) {
            origin = new GeoPoint(lat, lon);
            return this;
        }

        /**
         * Add the range of the group
         *
         * @param from The starting value, in meters. You can use the minimum value <pre> Double.MIN_VALUE</pre>
         * @param to   The ending value, in meters. You can use the maximum value <pre> Double.MAX_VALUE</pre>
         * @return The current builder
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
        public GroupByGeoDistance build() {
            return new GroupByGeoDistance(this);
        }
    }
}
