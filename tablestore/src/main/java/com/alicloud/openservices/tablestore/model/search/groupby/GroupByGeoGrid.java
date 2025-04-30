package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder;
import com.alicloud.openservices.tablestore.model.search.GeoHashPrecision;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Group and count fields of GeoPoint type by geographic area
 */
public class GroupByGeoGrid implements GroupBy {

    private final GroupByType groupByType = GroupByType.GROUP_BY_GEO_GRID;

    /**
     * The name of the GroupBy, which is used to retrieve the GroupBy result from the GroupBy result list later.
     */
    private String groupByName;
    /**
     * Field name
     */
    private String fieldName;
    /**
     * Precision of GroupBy
     */
    private GeoHashPrecision precision;
    /**
     * Returns the number of groups
     */
    private Integer size;
    /**
     * Sub-aggregation
     */
    private List<Aggregation> subAggregations;
    /**
     * Sub grouping
     */
    private List<GroupBy> subGroupBys;


    public GroupByGeoGrid() {
    }

    public GroupByGeoGrid(Builder builder) {
        this.groupByName = builder.groupByName;
        this.fieldName = builder.fieldName;
        this.precision = builder.precision;
        this.size = builder.size;
        this.subAggregations = builder.subAggregations;
        this.subGroupBys = builder.subGroupBys;
    }

    public static GroupByGeoGrid.Builder newBuilder() {
        return new Builder();
    }

    @Override
    public GroupByType getGroupByType() {
        return groupByType;
    }

    @Override
    public String getGroupByName() {
        return groupByName;
    }

    public GroupByGeoGrid setGroupByName(String groupByName) {
        this.groupByName = groupByName;
        return this;
    }

    public String getFieldName() {
        return fieldName;
    }

    public GroupByGeoGrid setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public GeoHashPrecision getPrecision() {
        return precision;
    }

    public GroupByGeoGrid setPrecision(GeoHashPrecision precision) {
        this.precision = precision;
        return this;
    }

    public Integer getSize() {
        return size;
    }

    public GroupByGeoGrid setSize(Integer size) {
        this.size = size;
        return this;
    }

    public List<Aggregation> getSubAggregations() {
        return subAggregations;
    }

    public GroupByGeoGrid setSubAggregations(List<Aggregation> subAggregations) {
        this.subAggregations = subAggregations;
        return this;
    }

    public List<GroupBy> getSubGroupBys() {
        return subGroupBys;
    }

    public GroupByGeoGrid setSubGroupBys(List<GroupBy> subGroupBys) {
        this.subGroupBys = subGroupBys;
        return this;
    }

    @Override
    public ByteString serialize() {
        return SearchGroupByBuilder.buildGroupByGeoGrid(this).toByteString();
    }

    public static final class Builder implements GroupByBuilder {
        private String groupByName;
        private String fieldName;
        private GeoHashPrecision precision;
        private Integer size;
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

        public Builder precision(GeoHashPrecision precision) {
            this.precision = precision;
            return this;
        }

        public Builder size(Integer size) {
            this.size = size;
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
        public GroupByGeoGrid build() {
            return new GroupByGeoGrid(this);
        }
    }
}
