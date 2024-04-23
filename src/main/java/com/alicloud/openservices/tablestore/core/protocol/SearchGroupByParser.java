package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.alicloud.openservices.tablestore.model.search.GeoHashPrecision;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistance;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoGrid;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogram;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRange;
import com.alicloud.openservices.tablestore.model.search.groupby.Range;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByComposite;
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

    private static GroupByComposite toGroupByComposite(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByComposite pb = Search.GroupByComposite.parseFrom(groupByByteString);
        GroupByComposite groupByComposite = new GroupByComposite();
        groupByComposite.setGroupByName(groupByName);
        if (pb.hasSize()) {
            groupByComposite.setSize(pb.getSize());
        }
        if (pb.hasNextToken()) {
            groupByComposite.setNextToken(pb.getNextToken());
        }
        if (pb.hasSources()) {
            groupByComposite.setSources(toGroupBys(pb.getSources()));
        }
        if (pb.hasSubGroupBys()) {
            groupByComposite.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupByComposite.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
        }

        return groupByComposite;
    }

    private static GroupByDateHistogram toGroupByDateHistogram(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByDateHistogram pb = Search.GroupByDateHistogram.parseFrom(groupByByteString);
        GroupByDateHistogram groupBy = new GroupByDateHistogram();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        if (pb.hasInterval()) {
            DateTimeValue dateTimeValue = SearchProtocolParser.toDateTimeValue(pb.getInterval());
            groupBy.setInterval(dateTimeValue);
        }
        if (pb.hasMissing()) {
            ColumnValue missing = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getMissing().toByteArray()));
            groupBy.setMissing(missing);
        }
        if (pb.hasOffset()) {
            DateTimeValue dateTimeValue = SearchProtocolParser.toDateTimeValue(pb.getOffset());
            groupBy.setOffset(dateTimeValue);
        }
        if (pb.hasSort()) {
            groupBy.setGroupBySorters(SearchSortParser.toGroupBySort(pb.getSort()));
        }
        if (pb.hasMinDocCount()) {
            groupBy.setMinDocCount(pb.getMinDocCount());
        }
        if (pb.hasTimeZone()) {
            groupBy.setTimeZone(pb.getTimeZone());
        }
        if (pb.hasFieldRange()) {
            Search.FieldRange fieldRangePb = pb.getFieldRange();
            ColumnValue min = ValueUtil.toColumnValue(SearchVariantType.getValue(fieldRangePb.getMin().toByteArray()));
            ColumnValue max = ValueUtil.toColumnValue(SearchVariantType.getValue(fieldRangePb.getMax().toByteArray()));
            groupBy.setFieldRange(new com.alicloud.openservices.tablestore.model.search.groupby.FieldRange(min, max));
        }
        if (pb.hasSubGroupBys()) {
            groupBy.setSubGroupBys(toGroupBys(pb.getSubGroupBys()));
        }
        if (pb.hasSubAggs()) {
            groupBy.setSubAggregations(SearchAggregationParser.toAggregations(pb.getSubAggs()));
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
        if (pb.hasOffset()) {
            ColumnValue columnValue = ValueUtil.toColumnValue(SearchVariantType.getValue(pb.getOffset().toByteArray()));
            groupBy.setOffset(columnValue);
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

    private static GeoHashPrecision toGeoHashPrecision(Search.GeoHashPrecision pbGeoHashPrecision) {
        switch (pbGeoHashPrecision) {
            case GHP_5009KM_4992KM_1:
                return GeoHashPrecision.GHP_5009KM_4992KM_1;
            case GHP_1252KM_624KM_2:
                return GeoHashPrecision.GHP_1252KM_624KM_2;
            case GHP_156KM_156KM_3:
                return GeoHashPrecision.GHP_156KM_156KM_3;
            case GHP_39KM_19KM_4:
                return GeoHashPrecision.GHP_39KM_19KM_4;
            case GHP_4900M_4900M_5:
                return GeoHashPrecision.GHP_4900M_4900M_5;
            case GHP_1200M_609M_6:
                return GeoHashPrecision.GHP_1200M_609M_6;
            case GHP_152M_152M_7:
                return GeoHashPrecision.GHP_152M_152M_7;
            case GHP_38M_19M_8:
                return GeoHashPrecision.GHP_38M_19M_8;
            case GHP_480CM_480CM_9:
                return GeoHashPrecision.GHP_480CM_480CM_9;
            case GHP_120CM_595MM_10:
                return GeoHashPrecision.GHP_120CM_595MM_10;
            case GHP_149MM_149MM_11:
                return GeoHashPrecision.GHP_149MM_149MM_11;
            case GHP_37MM_19MM_12:
                return GeoHashPrecision.GHP_37MM_19MM_12;
            default:
                return GeoHashPrecision.UNKNOWN;
        }
    }

    private static GroupByGeoGrid toGroupByGeoGrid(String groupByName, ByteString groupByByteString) throws IOException {
        Search.GroupByGeoGrid pb = Search.GroupByGeoGrid.parseFrom(groupByByteString);
        GroupByGeoGrid groupBy = new GroupByGeoGrid();
        groupBy.setGroupByName(groupByName);
        if (pb.hasFieldName()) {
            groupBy.setFieldName(pb.getFieldName());
        }
        if (pb.hasPrecision()) {
            groupBy.setPrecision(toGeoHashPrecision(pb.getPrecision()));
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
            case GROUP_BY_DATE_HISTOGRAM:
                return toGroupByDateHistogram(groupByName, body);
            case GROUP_BY_GEO_GRID:
                return toGroupByGeoGrid(groupByName, body);
            case GROUP_BY_COMPOSITE:
                return toGroupByComposite(groupByName, body);
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
