package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogramItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByDateHistogramResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFieldResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByFilterResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistanceResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoDistanceResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByHistogramResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByRangeResultItem;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByResult;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByResults;
import com.google.protobuf.ByteString;

import static com.alicloud.openservices.tablestore.core.protocol.SearchAggregationResultBuilder.buildAggregationResults;

class SearchGroupByResultBuilder {

    private static GroupByResult buildGroupByFieldResult(String groupByName, ByteString groupByBody)
        throws IOException {
        Search.GroupByFieldResult groupByResult = Search.GroupByFieldResult.parseFrom(groupByBody);
        GroupByFieldResult result = new GroupByFieldResult();
        result.setGroupByName(groupByName);

        List<GroupByFieldResultItem> items = new ArrayList<GroupByFieldResultItem>();
        for (Search.GroupByFieldResultItem item : groupByResult.getGroupByFieldResultItemsList()) {
            items.add(buildGroupByFieldResultItem(item));
        }
        result.setGroupByFieldResultItems(items);
        return result;
    }

    private static GroupByFieldResultItem buildGroupByFieldResultItem(
            Search.GroupByFieldResultItem fieldGroupByResultItem) throws IOException {
        GroupByFieldResultItem result = new GroupByFieldResultItem();
        result.setKey(fieldGroupByResultItem.getKey());
        result.setRowCount(fieldGroupByResultItem.getRowCount());
        if (fieldGroupByResultItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(fieldGroupByResultItem.getSubAggsResult()));
        }
        if (fieldGroupByResultItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(fieldGroupByResultItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByRangeResultItem buildGroupByRangeResultItem(
            Search.GroupByRangeResultItem rangeGroupByResultItem) throws IOException {
        GroupByRangeResultItem result = new GroupByRangeResultItem();
        result.setRowCount(rangeGroupByResultItem.getRowCount());
        result.setFrom(rangeGroupByResultItem.getFrom());
        result.setTo(rangeGroupByResultItem.getTo());
        if (rangeGroupByResultItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(rangeGroupByResultItem.getSubAggsResult()));
        }
        if (rangeGroupByResultItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(rangeGroupByResultItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByHistogramItem buildGroupByHistogramItem(
        Search.GroupByHistogramItem groupByItem) throws IOException {
        GroupByHistogramItem result = new GroupByHistogramItem();
        result.setKey(SearchVariantType.forceConvertToDestColumnValue(groupByItem.getKey().toByteArray()));
        result.setValue(groupByItem.getValue());

        if (groupByItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(groupByItem.getSubAggsResult()));
        }
        if (groupByItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(groupByItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByGeoDistanceResultItem buildGroupByGeoDistanceResultItem(
            Search.GroupByGeoDistanceResultItem geoDistanceResultItem) throws IOException {
        GroupByGeoDistanceResultItem result = new GroupByGeoDistanceResultItem();
        result.setRowCount(geoDistanceResultItem.getRowCount());
        result.setFrom(geoDistanceResultItem.getFrom());
        result.setTo(geoDistanceResultItem.getTo());
        if (geoDistanceResultItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(geoDistanceResultItem.getSubAggsResult()));
        }
        if (geoDistanceResultItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(geoDistanceResultItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByFilterResultItem buildGroupByFilterResultItem(
            Search.GroupByFilterResultItem filtersGroupByResultItem) throws IOException {
        GroupByFilterResultItem result = new GroupByFilterResultItem();
        result.setRowCount(filtersGroupByResultItem.getRowCount());
        if (filtersGroupByResultItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(filtersGroupByResultItem.getSubAggsResult()));
        }
        if (filtersGroupByResultItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(filtersGroupByResultItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByFilterResult buildGroupByFilterResult(String groupByName, ByteString groupByBody)
        throws IOException {
        Search.GroupByFilterResult groupByResult = Search.GroupByFilterResult.parseFrom(groupByBody);
        GroupByFilterResult result = new GroupByFilterResult();
        result.setGroupByName(groupByName);

        List<GroupByFilterResultItem> items = new ArrayList<GroupByFilterResultItem>();
        for (Search.GroupByFilterResultItem item : groupByResult.getGroupByFilterResultItemsList()) {
            items.add(buildGroupByFilterResultItem(item));
        }
        result.setGroupByFilterResultItems(items);
        return result;
    }

    private static GroupByRangeResult buildGroupByRangeResult(String groupByName, ByteString groupByBody)
        throws IOException {
        Search.GroupByRangeResult groupByResult = Search.GroupByRangeResult.parseFrom(groupByBody);
        GroupByRangeResult result = new GroupByRangeResult();
        result.setGroupByName(groupByName);

        List<GroupByRangeResultItem> items = new ArrayList<GroupByRangeResultItem>();
        for (Search.GroupByRangeResultItem item : groupByResult.getGroupByRangeResultItemsList()) {
            items.add(buildGroupByRangeResultItem(item));
        }
        result.setGroupByRangeResultItems(items);
        return result;
    }

    private static GroupByHistogramResult buildGroupByHistogramResult(String groupByName, ByteString groupByBody)
        throws IOException {
        Search.GroupByHistogramResult groupByResult = Search.GroupByHistogramResult.parseFrom(groupByBody);
        GroupByHistogramResult result = new GroupByHistogramResult();
        result.setGroupByName(groupByName);

        List<GroupByHistogramItem> items = new ArrayList<GroupByHistogramItem>();
        for (Search.GroupByHistogramItem item : groupByResult.getGroupByHistograItemsList()) {
            items.add(buildGroupByHistogramItem(item));
        }
        result.setGroupByHistogramItems(items);
        return result;
    }

    private static GroupByDateHistogramResult buildGroupByDateHistogramResult(String groupByName, ByteString groupByBody) throws IOException {
        Search.GroupByDateHistogramResult groupByResult = Search.GroupByDateHistogramResult.parseFrom(groupByBody);
        GroupByDateHistogramResult result = new GroupByDateHistogramResult();
        result.setGroupByName(groupByName);

        List<GroupByDateHistogramItem> items = new ArrayList<GroupByDateHistogramItem>();
        for (Search.GroupByDateHistogramItem item : groupByResult.getGroupByDateHistogramItemsList()) {
            items.add(buildGroupByDateHistogramItem(item));
        }
        result.setGroupByDateHistogramItems(items);
        return result;
    }

    private static GroupByDateHistogramItem buildGroupByDateHistogramItem(Search.GroupByDateHistogramItem groupByItem) throws IOException {
        GroupByDateHistogramItem result = new GroupByDateHistogramItem();
        result.setTimestamp(groupByItem.getTimestamp());
        result.setRowCount(groupByItem.getRowCount());

        if (groupByItem.hasSubAggsResult()) {
            result.setSubAggregationResults(buildAggregationResults(groupByItem.getSubAggsResult()));
        }
        if (groupByItem.hasSubGroupBysResult()) {
            result.setSubGroupByResults(buildGroupByResults(groupByItem.getSubGroupBysResult()));
        }
        return result;
    }

    private static GroupByGeoDistanceResult buildGroupByGeoDistanceResult(String groupByName, ByteString groupByBody)
        throws IOException {
        Search.GroupByGeoDistanceResult groupByResult = Search.GroupByGeoDistanceResult.parseFrom(groupByBody);
        GroupByGeoDistanceResult result = new GroupByGeoDistanceResult();
        result.setGroupByName(groupByName);

        List<GroupByGeoDistanceResultItem> items = new ArrayList<GroupByGeoDistanceResultItem>();
        for (Search.GroupByGeoDistanceResultItem item : groupByResult.getGroupByGeoDistanceResultItemsList()) {
            items.add(buildGroupByGeoDistanceResultItem(item));
        }
        result.setGroupByGeoDistanceResultItems(items);
        return result;
    }

    private static GroupByResult buildGroupByResult(Search.GroupByResult groupByResult)
        throws IOException {
        switch (groupByResult.getType()) {
            case GROUP_BY_FIELD:
                return buildGroupByFieldResult(groupByResult.getName(), groupByResult.getGroupByResult());
            case GROUP_BY_RANGE:
                return buildGroupByRangeResult(groupByResult.getName(), groupByResult.getGroupByResult());
            case GROUP_BY_GEO_DISTANCE:
                return buildGroupByGeoDistanceResult(groupByResult.getName(), groupByResult.getGroupByResult());
            case GROUP_BY_FILTER:
                return buildGroupByFilterResult(groupByResult.getName(), groupByResult.getGroupByResult());
            case GROUP_BY_HISTOGRAM:
                return buildGroupByHistogramResult(groupByResult.getName(), groupByResult.getGroupByResult());
            case GROUP_BY_DATE_HISTOGRAM:
                return buildGroupByDateHistogramResult(groupByResult.getName(), groupByResult.getGroupByResult());
            default:
                throw new ClientException("unsupported GroupByType: " + groupByResult.getType());
        }
    }

    private static GroupByResults buildGroupByResults(Search.GroupBysResult groupBysResult)
        throws IOException {
        GroupByResults groupByResults = new GroupByResults();
        Map<String, GroupByResult> map = new HashMap<String, GroupByResult>();

        for (Search.GroupByResult groupByResult : groupBysResult.getGroupByResultsList()) {
            map.put(groupByResult.getName(), buildGroupByResult(groupByResult));
        }

        groupByResults.setGroupByResultMap(map);
        return groupByResults;
    }

    static GroupByResults buildGroupByResultsFromByteString(ByteString groupBy)
        throws IOException {
        Search.GroupBysResult aggregationsResult = Search.GroupBysResult.parseFrom(groupBy);
        return buildGroupByResults(aggregationsResult);
    }

}
