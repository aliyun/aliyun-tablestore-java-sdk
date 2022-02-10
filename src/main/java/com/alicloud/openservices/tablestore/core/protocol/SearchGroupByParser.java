package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistance;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRange;
import com.alicloud.openservices.tablestore.model.search.groupby.Range;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link GroupBy} deserialization tool class. For serialization, please refer to {@link SearchGroupByBuilder}
 */
public class SearchGroupByParser {

    private static GroupByField toGroupByField(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByField pb = Search.GroupByField.parseFrom(groupByByteString);
        GroupByField groupBy = new GroupByField();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        if (pb.hasSort()) {
            groupBy.setGroupBySorters(SearchSortParser.toGroupBySort(pb.getSort()));
        }
        if (pb.hasSize()) {
            groupBy.setSize(pb.getSize());
        }
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }
        if (pb.hasMinDocCount()) {
            groupBy.setMinDocCount(pb.getMinDocCount());
        }
        return groupBy;
    }

    private static GroupByHistogram toGroupByHistogram(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByHistogram pb = Search.GroupByHistogram.parseFrom(groupByByteString);
        GroupByHistogram groupBy = new GroupByHistogram();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        if (pb.hasSort()) {
            groupBy.setGroupBySorters(SearchSortParser.toGroupBySort(pb.getSort()));
        }
        if (pb.hasInterval()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getInterval().toByteArray()));
            groupBy.setInterval(columnValue);
        }
        if (pb.hasFieldRange()) {
            Search.FieldRange fieldRangePb = pb.getFieldRange();
            ColumnValue min = ValueUtil.toColumnValue(SearchVariantType.getValue(fieldRangePb.getMin().toByteArray()));
            ColumnValue max = ValueUtil.toColumnValue(SearchVariantType.getValue(fieldRangePb.getMax().toByteArray()));
            groupBy.setFieldRange(new com.alicloud.openservices.tablestore.model.search.groupby.FieldRange(min, max));
        }
        if (pb.hasMissing()) {
            ColumnValue missing = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            groupBy.setMissing(missing);
        }
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }
        if (pb.hasMinDocCount()) {
            groupBy.setMinDocCount(pb.getMinDocCount());
        }
        return groupBy;
    }

    private static GroupByGeoDistance toGroupByGeoDistance(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByGeoDistance pb = Search.GroupByGeoDistance.parseFrom(groupByByteString);
        GroupByGeoDistance groupBy = new GroupByGeoDistance();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        if (pb.hasOrigin()) {
            groupBy.setOrigin(toGeoPoint(pb.getOrigin()));
        }
        List<Search.Range> rangesList = pb.getRangesList();
        List<Range> list = new ArrayList<Range>();
        for (Search.Range range : rangesList) {
            list.add(toRange(range));
        }
        groupBy.setRanges(list);
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }
        return groupBy;
    }

    private static GroupByRange toGroupByRange(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByRange pb = Search.GroupByRange.parseFrom(groupByByteString);
        GroupByRange groupBy = new GroupByRange();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        List<Search.Range> rangesList = pb.getRangesList();
        List<Range> list = new ArrayList<Range>();
        for (Search.Range range : rangesList) {
            list.add(toRange(range));
        }
        groupBy.setRanges(list);
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }
        return groupBy;
    }

    private static GroupByFilter toGroupByFilter(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByFilter pb = Search.GroupByFilter.parseFrom(groupByByteString);
        GroupByFilter groupBy = new GroupByFilter();
        groupBy.setGroupByName(groupByName);
        List<Search.Query> rangesList = pb.getFiltersList();
        List<Query> list = new ArrayList<Query>();
        for (Search.Query query : rangesList) {
            list.add(SearchQueryParser.toQuery(query));
        }
        groupBy.setFilters(list);
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }
        return groupBy;
    }

    private static Range toRange(Search.Range pb) {
        Preconditions.checkArgument(pb.hasFrom(), "Search.Range must has 'from'");
        Preconditions.checkArgument(pb.hasTo(), "Search.Range must has 'to'");
        return new Range(pb.getFrom(), pb.getTo());
    }

    private static GeoPoint toGeoPoint(Search.GeoPoint pb) {
        Preconditions.checkArgument(pb.hasLat(), "Search.GeoPoint must has 'lat'");
        Preconditions.checkArgument(pb.hasLon(), "Search.GeoPoint must has 'lon'");
        return new GeoPoint(pb.getLat(), pb.getLon());
    }

    public static GroupBy toGroupBy(Search.GroupBy pb) throws IOException {
        String groupByName = pb.getName();
        ByteString body = pb.getBody();
        Search.GroupByType type = pb.getType();
        switch (type) {
            case GROUP_BY_FIELD:
                return toGroupByField(groupByName, body);
            case GROUP_BY_GEO_DISTANCE:
                return toGroupByGeoDistance(groupByName, body);
            case GROUP_BY_RANGE:
                return toGroupByRange(groupByName, body);
            case GROUP_BY_FILTER:
                return toGroupByFilter(groupByName, body);
            case GROUP_BY_HISTOGRAM:
                return toGroupByHistogram(groupByName, body);
            default:
                throw new IllegalArgumentException("unknown GroupByType: " + type.name());
        }
    }


    public static List<GroupBy> toGroupBys(Search.GroupBys pb) throws IOException {
        List<GroupBy> groupBys = new ArrayList<GroupBy>();
        for (Search.GroupBy groupBy : pb.getGroupBysList()) {
            groupBys.add(toGroupBy(groupBy));
        }
        return groupBys;
    }

}
