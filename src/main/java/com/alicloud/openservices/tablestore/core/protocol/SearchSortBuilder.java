package com.alicloud.openservices.tablestore.core.protocol;

import java.util.List;

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
import com.alicloud.openservices.tablestore.model.search.sort.SortMode;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.model.search.sort.SubAggSort;
import com.google.protobuf.ByteString;

class SearchSortBuilder {

    private static Search.SortOrder buildSortOrder(SortOrder sortOrder) {
        switch (sortOrder) {
            case ASC:
                return Search.SortOrder.SORT_ORDER_ASC;
            case DESC:
                return Search.SortOrder.SORT_ORDER_DESC;
            default:
                throw new IllegalArgumentException("Unknown sortOrder:" + sortOrder.name());
        }
    }

    private static Search.SortMode buildSortMode(SortMode sortMode) {
        switch (sortMode) {
            case MIN:
                return Search.SortMode.SORT_MODE_MIN;
            case MAX:
                return Search.SortMode.SORT_MODE_MAX;
            case AVG:
                return Search.SortMode.SORT_MODE_AVG;
            default:
                throw new IllegalArgumentException("Unknown sortMode:" + sortMode.name());
        }
    }

    private static Search.NestedFilter buildNestedFilter(NestedFilter nestedFilter) {
        Search.NestedFilter.Builder builder = Search.NestedFilter.newBuilder();
        builder.setPath(nestedFilter.getPath());
        builder.setFilter(SearchQueryBuilder.buildQuery(nestedFilter.getQuery()));
        return builder.build();
    }

    private static Search.FieldSort buildFieldSort(FieldSort fieldSort) {
        Search.FieldSort.Builder builder = Search.FieldSort.newBuilder();
        builder.setFieldName(fieldSort.getFieldName());
        if (fieldSort.getOrder() != null) {
            builder.setOrder(buildSortOrder(fieldSort.getOrder()));
        }
        if (fieldSort.getMode() != null) {
            builder.setMode(buildSortMode(fieldSort.getMode()));
        }
        if (fieldSort.getNestedFilter() != null) {
            builder.setNestedFilter(buildNestedFilter(fieldSort.getNestedFilter()));
        }
        if (fieldSort.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(fieldSort.getMissing())));
        }
        return builder.build();
    }

    private static Search.ScoreSort buildScoreSort(ScoreSort scoreSort) {
        Search.ScoreSort.Builder builder = Search.ScoreSort.newBuilder();
        builder.setOrder(buildSortOrder(scoreSort.getOrder()));
        return builder.build();
    }

    private static Search.GeoDistanceType buildGeoDistanceType(GeoDistanceType geoDistanceType) {
        switch (geoDistanceType) {
            case ARC:
                return Search.GeoDistanceType.GEO_DISTANCE_ARC;
            case PLANE:
                return Search.GeoDistanceType.GEO_DISTANCE_PLANE;
            default:
                throw new IllegalArgumentException("unknown geoDistanceType: " + geoDistanceType.name());
        }
    }

    private static Search.GeoDistanceSort buildGeoDistanceSort(GeoDistanceSort geoDistanceSort) {
        Search.GeoDistanceSort.Builder builder = Search.GeoDistanceSort.newBuilder();
        builder.setFieldName(geoDistanceSort.getFieldName());
        if (geoDistanceSort.getPoints() != null) {
            builder.addAllPoints(geoDistanceSort.getPoints());
        }
        if (geoDistanceSort.getOrder() != null) {
            builder.setOrder(buildSortOrder(geoDistanceSort.getOrder()));
        }
        if (geoDistanceSort.getMode() != null) {
            builder.setMode(buildSortMode(geoDistanceSort.getMode()));
        }
        if (geoDistanceSort.getDistanceType() != null) {
            builder.setDistanceType(buildGeoDistanceType(geoDistanceSort.getDistanceType()));
        }
        if (geoDistanceSort.getNestedFilter() != null) {
            builder.setNestedFilter(buildNestedFilter(geoDistanceSort.getNestedFilter()));
        }
        return builder.build();
    }

    private static Search.PrimaryKeySort buildPrimaryKeySort(PrimaryKeySort primaryKeySort) {
        Search.PrimaryKeySort.Builder builder = Search.PrimaryKeySort.newBuilder();
        builder.setOrder(buildSortOrder(primaryKeySort.getOrder()));
        return builder.build();
    }

    private static Search.Sorter buildSorter(Sort.Sorter sorter) {
        Search.Sorter.Builder builder = Search.Sorter.newBuilder();
        if (sorter instanceof FieldSort) {
            builder.setFieldSort(buildFieldSort((FieldSort)sorter));
        } else if (sorter instanceof ScoreSort) {
            builder.setScoreSort(buildScoreSort((ScoreSort)sorter));
        } else if (sorter instanceof GeoDistanceSort) {
            builder.setGeoDistanceSort(buildGeoDistanceSort((GeoDistanceSort)sorter));
        } else if (sorter instanceof PrimaryKeySort) {
            builder.setPkSort(buildPrimaryKeySort((PrimaryKeySort)sorter));
        } else {
            throw new IllegalArgumentException("Unknown sorter type: " + sorter.getClass());
        }
        return builder.build();
    }

    static Search.Sort buildSort(Sort sort) {
        Search.Sort.Builder builder = Search.Sort.newBuilder();
        for (Sort.Sorter sorter : sort.getSorters()) {
            builder.addSorter(buildSorter(sorter));
        }
        return builder.build();
    }

    static Search.GroupBySort buildGroupBySort(List<GroupBySorter> groupBySorters) {
        Search.GroupBySort.Builder builder = Search.GroupBySort.newBuilder();
        for (GroupBySorter groupBySorter : groupBySorters) {
            builder.addSorters(buildGroupBySorter(groupBySorter));
        }
        return builder.build();
    }

    private static Search.GroupBySorter buildGroupBySorter(GroupBySorter groupBySorter) {
        Search.GroupBySorter.Builder builder = Search.GroupBySorter.newBuilder();
        if (groupBySorter.getGroupKeySort() != null) {
            builder.setGroupKeySort(buildGroupKeySort(groupBySorter.getGroupKeySort()));
        }
        if (groupBySorter.getRowCountSort() != null) {
            builder.setRowCountSort(buildRowCountSort(groupBySorter.getRowCountSort()));
        }
        if (groupBySorter.getSubAggSort() != null) {
            builder.setSubAggSort(buildSubAggSort(groupBySorter.getSubAggSort()));
        }
        return builder.build();
    }

    private static Search.GroupKeySort buildGroupKeySort(GroupKeySort groupKeySort) {
        Search.GroupKeySort.Builder builder = Search.GroupKeySort.newBuilder();
        builder.setOrder(buildSortOrder(groupKeySort.getOrder()));
        return builder.build();
    }

    private static Search.RowCountSort buildRowCountSort(RowCountSort rowCountSort) {
        Search.RowCountSort.Builder builder = Search.RowCountSort.newBuilder();
        builder.setOrder(buildSortOrder(rowCountSort.getOrder()));
        return builder.build();
    }

    private static Search.SubAggSort buildSubAggSort(SubAggSort subAggSort) {
        Search.SubAggSort.Builder builder = Search.SubAggSort.newBuilder();
        builder.setOrder(buildSortOrder(subAggSort.getOrder()));
        builder.setSubAggName(subAggSort.getSubAggName());
        return builder.build();
    }

}
