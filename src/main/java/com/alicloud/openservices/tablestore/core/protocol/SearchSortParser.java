package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceSort;
import com.alicloud.openservices.tablestore.model.search.sort.GeoDistanceType;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.GroupKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.NestedFilter;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.RowCountSort;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort.Sorter;
import com.alicloud.openservices.tablestore.model.search.sort.SortMode;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.model.search.sort.SubAggSort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class SearchSortParser {

    private static SortOrder toSortOrder(Search.SortOrder sortOrder) {
        switch (sortOrder) {
            case SORT_ORDER_ASC:
                return SortOrder.ASC;
            case SORT_ORDER_DESC:
                return SortOrder.DESC;
            default:
                throw new IllegalArgumentException("Unknown sortOrder:" + sortOrder.name());
        }
    }

    private static SortMode toSortMode(Search.SortMode sortMode) {
        switch (sortMode) {
            case SORT_MODE_MIN:
                return SortMode.MIN;
            case SORT_MODE_MAX:
                return SortMode.MAX;
            case SORT_MODE_AVG:
                return SortMode.AVG;
            default:
                throw new IllegalArgumentException("Unknown sortMode:" + sortMode.name());
        }
    }

    private static NestedFilter toNestedFilter(Search.NestedFilter pb) throws IOException {
        String path = pb.getPath();
        Query query = SearchQueryParser.toQuery(pb.getFilter());
        return new NestedFilter(path, query);
    }

    private static FieldSort toFieldSort(Search.FieldSort pb) throws IOException {
        FieldSort sort = new FieldSort(pb.getFieldName());
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        if (pb.hasMode()) {
            sort.setMode(toSortMode(pb.getMode()));
        }
        if (pb.hasNestedFilter()) {
            sort.setNestedFilter(toNestedFilter(pb.getNestedFilter()));
        }
        if (pb.hasMissing()) {
            sort.setMissing(SearchVariantType.forceConvertToDestColumnValue(pb.getMissing().toByteArray()));
        }
        return sort;
    }

    private static ScoreSort toScoreSort(Search.ScoreSort pb) {
        ScoreSort sort = new ScoreSort();
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        return sort;
    }

    private static GeoDistanceType toGeoDistanceType(Search.GeoDistanceType pb) {
        switch (pb) {
            case GEO_DISTANCE_ARC:
                return GeoDistanceType.ARC;
            case GEO_DISTANCE_PLANE:
                return GeoDistanceType.PLANE;
            default:
                throw new IllegalArgumentException("unknown geoDistanceType: " + pb.name());
        }
    }

    private static GeoDistanceSort toGeoDistanceSort(Search.GeoDistanceSort pb) throws IOException {
        String fieldName = pb.getFieldName();
        List<String> pointsList = pb.getPointsList();
        GeoDistanceSort sort = new GeoDistanceSort(fieldName, pointsList);
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        if (pb.hasMode()) {
            sort.setMode(toSortMode(pb.getMode()));
        }
        if (pb.hasDistanceType()) {
            sort.setDistanceType(toGeoDistanceType(pb.getDistanceType()));
        }
        if (pb.hasNestedFilter()) {
            sort.setNestedFilter(toNestedFilter(pb.getNestedFilter()));
        }
        return sort;
    }

    private static PrimaryKeySort toPrimaryKeySort(Search.PrimaryKeySort pb) {
        PrimaryKeySort sort = new PrimaryKeySort();
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        return sort;
    }

    private static Sort.Sorter toSorter(Search.Sorter pb) throws IOException {
        if (pb.hasFieldSort()) {
            return toFieldSort(pb.getFieldSort());
        }
        if (pb.hasPkSort()) {
            return toPrimaryKeySort(pb.getPkSort());
        }
        if (pb.hasScoreSort()) {
            return toScoreSort(pb.getScoreSort());
        }
        if (pb.hasGeoDistanceSort()) {
            return toGeoDistanceSort(pb.getGeoDistanceSort());
        }
        throw new IllegalArgumentException("can not parse a sorter from Search.Sorter");
    }

    static Sort toSort(Search.Sort pb) throws IOException {
        List<Sorter> sorters = new ArrayList<Sorter>();
        for (Search.Sorter sorter : pb.getSorterList()) {
            sorters.add(toSorter(sorter));
        }
        return new Sort(sorters);
    }

    static List<GroupBySorter> toGroupBySort(Search.GroupBySort groupBySort) {
        List<GroupBySorter> sorters = new ArrayList<GroupBySorter>();
        for (Search.GroupBySorter pb : groupBySort.getSortersList()) {
            sorters.add(toGroupBySorter(pb));
        }
        return sorters;
    }

    private static GroupBySorter toGroupBySorter(Search.GroupBySorter pb) {
        GroupBySorter sorter = new GroupBySorter();
        if (pb.hasGroupKeySort()) {
            sorter.setGroupKeySort(toGroupKeySort(pb.getGroupKeySort()));
        }
        if (pb.hasRowCountSort()) {
            sorter.setRowCountSort(toRowCountSort(pb.getRowCountSort()));
        }
        if (pb.hasSubAggSort()) {
            sorter.setSubAggSort(toSubAggSort(pb.getSubAggSort()));
        }
        return sorter;
    }

    private static GroupKeySort toGroupKeySort(Search.GroupKeySort pb) {
        GroupKeySort sort = new GroupKeySort();
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        return sort;
    }

    private static RowCountSort toRowCountSort(Search.RowCountSort pb) {
        RowCountSort sort = new RowCountSort();
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        return sort;
    }

    private static SubAggSort toSubAggSort(Search.SubAggSort pb) {
        SubAggSort sort = new SubAggSort();
        if (pb.hasOrder()) {
            sort.setOrder(toSortOrder(pb.getOrder()));
        }
        if (pb.hasSubAggName()) {
            sort.setSubAggName(pb.getSubAggName());
        }
        return sort;
    }
}
