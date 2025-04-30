package com.alicloud.openservices.tablestore.core.protocol;

import java.util.List;

import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationType;
import com.alicloud.openservices.tablestore.model.search.agg.AvgAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.CountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.DistinctCountAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.MinAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.PercentilesAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.agg.TopRowsAggregation;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * {@link Aggregation} serialization tool class. For deserialization, please refer to {@link SearchAggregationParser}
 */
public class SearchAggregationBuilder {

    private static Search.AggregationType buildAggregationType(AggregationType type) {
        switch (type) {
            case AGG_AVG:
                return Search.AggregationType.AGG_AVG;
            case AGG_MIN:
                return Search.AggregationType.AGG_MIN;
            case AGG_MAX:
                return Search.AggregationType.AGG_MAX;
            case AGG_SUM:
                return Search.AggregationType.AGG_SUM;
            case AGG_COUNT:
                return Search.AggregationType.AGG_COUNT;
            case AGG_DISTINCT_COUNT:
                return Search.AggregationType.AGG_DISTINCT_COUNT;
            case AGG_TOP_ROWS:
                return Search.AggregationType.AGG_TOP_ROWS;
            case AGG_PERCENTILES:
                return Search.AggregationType.AGG_PERCENTILES;
            default:
                throw new IllegalArgumentException("unknown AggregationType: " + type.name());
        }
    }

    public static Search.MaxAggregation buildMaxAggregation(MaxAggregation agg) {
        Search.MaxAggregation.Builder builder = Search.MaxAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    public static Search.AvgAggregation buildAvgAggregation(AvgAggregation agg) {
        Search.AvgAggregation.Builder builder = Search.AvgAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    public static Search.MinAggregation buildMinAggregation(MinAggregation agg) {
        Search.MinAggregation.Builder builder = Search.MinAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    public static Search.SumAggregation buildSumAggregation(SumAggregation agg) {
        Search.SumAggregation.Builder builder = Search.SumAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    public static Search.CountAggregation buildCountAggregation(CountAggregation agg) {
        Search.CountAggregation.Builder builder = Search.CountAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        return builder.build();
    }

    public static Search.DistinctCountAggregation buildDistinctCountAggregation(DistinctCountAggregation agg) {
        Search.DistinctCountAggregation.Builder builder = Search.DistinctCountAggregation.newBuilder();
        builder.setFieldName(agg.getFieldName());
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    public static Search.TopRowsAggregation buildTopRowsAggregation(TopRowsAggregation agg) {
        Search.TopRowsAggregation.Builder builder = Search.TopRowsAggregation.newBuilder();
        if (agg.getLimit() != null) {
            builder.setLimit(agg.getLimit());
        }
        if (agg.getSort() != null) {
            builder.setSort(SearchSortBuilder.buildSort(agg.getSort()));
        }
        return builder.build();
    }

    public static Search.PercentilesAggregation buildPercentilesAggregation(PercentilesAggregation agg) {
        Search.PercentilesAggregation.Builder builder = Search.PercentilesAggregation.newBuilder();
        if (agg.getFieldName() != null) {
            builder.setFieldName(agg.getFieldName());
        }
        if (agg.getPercentiles() != null) {
            builder.addAllPercentiles(agg.getPercentiles());
        }
        if (agg.getMissing() != null) {
            builder.setMissing(ByteString.copyFrom(SearchVariantType.toVariant(agg.getMissing())));
        }
        return builder.build();
    }

    private static Search.Aggregation buildAggregation(Aggregation aggregation) {
        Search.Aggregation.Builder builder = Search.Aggregation.newBuilder();
        builder.setName(aggregation.getAggName());
        builder.setType(buildAggregationType(aggregation.getAggType()));
        builder.setBody(aggregation.serialize());
        return builder.build();
    }

    public static Search.Aggregations buildAggregations(List<Aggregation> aggregations) {
        Search.Aggregations.Builder builder = Search.Aggregations.newBuilder();
        for (Aggregation aggregation : aggregations) {
            builder.addAggs(buildAggregation(aggregation));
        }
        return builder.build();
    }

}
