package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.sort.*;

public class SearchSortBuilder {

    public static Search.SortOrder buildSortOrder(SortOrder sortOrder) {
        switch (sortOrder) {
            case ASC:
                return Search.SortOrder.SORT_ORDER_ASC;
            case DESC:
                return Search.SortOrder.SORT_ORDER_DESC;
            default:
                throw new IllegalArgumentException("Unknown sortOrder:" + sortOrder.name());
        }
    }

    public static Search.SortMode buildSortMode(SortMode sortMode) {
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

    public static Search.NestedFilter buildNestedFilter(NestedFilter nestedFilter) {
        Search.NestedFilter.Builder builder = Search.NestedFilter.newBuilder();
        builder.setPath(nestedFilter.getPath());
        builder.setFilter(SearchQueryBuilder.buildQuery(nestedFilter.getQuery()));
        return builder.build();
    }

    public static Search.FieldSort buildFieldSort(FieldSort fieldSort) {
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
        return builder.build();
    }

    public static Search.ScoreSort buildScoreSort(ScoreSort scoreSort) {
        Search.ScoreSort.Builder builder = Search.ScoreSort.newBuilder();
        builder.setOrder(buildSortOrder(scoreSort.getOrder()));
        return builder.build();
    }

    public static Search.GeoDistanceType buildGeoDistanceType(GeoDistanceType geoDistanceType) {
        switch (geoDistanceType) {
            case ARC:
                return Search.GeoDistanceType.GEO_DISTANCE_ARC;
            case PLANE:
                return Search.GeoDistanceType.GEO_DISTANCE_PLANE;
            default:
                throw new IllegalArgumentException("unknown geoDistanceType: " + geoDistanceType.name());
        }
    }

    public static Search.GeoDistanceSort buildGeoDistanceSort(GeoDistanceSort geoDistanceSort) {
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

    public static Search.PrimaryKeySort buildPrimaryKeySort(PrimaryKeySort primaryKeySort) {
        Search.PrimaryKeySort.Builder builder = Search.PrimaryKeySort.newBuilder();
        builder.setOrder(buildSortOrder(primaryKeySort.getOrder()));
        return builder.build();
    }

    public static Search.Sorter buildSorter(Sort.Sorter sorter) {
        Search.Sorter.Builder builder = Search.Sorter.newBuilder();
        if (sorter instanceof FieldSort) {
            builder.setFieldSort(buildFieldSort((FieldSort) sorter));
        } else if (sorter instanceof ScoreSort) {
            builder.setScoreSort(buildScoreSort((ScoreSort) sorter));
        } else if (sorter instanceof GeoDistanceSort) {
            builder.setGeoDistanceSort(buildGeoDistanceSort((GeoDistanceSort) sorter));
        } else if (sorter instanceof PrimaryKeySort) {
            builder.setPkSort(buildPrimaryKeySort((PrimaryKeySort) sorter));
        } else {
            throw new IllegalArgumentException("Unknown sorter type: " + sorter.getClass());
        }
        return builder.build();
    }

    public static Search.Sort buildSort(Sort sort) {
        Search.Sort.Builder builder = Search.Sort.newBuilder();
        for (Sort.Sorter sorter : sort.getSorters()) {
            builder.addSorter(buildSorter(sorter));
        }
        return builder.build();
    }

}
