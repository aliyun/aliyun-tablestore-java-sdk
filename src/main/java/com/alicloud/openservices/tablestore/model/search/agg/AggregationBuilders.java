package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * Aggregation 总的构建器。
 * 所有的 Aggregation 进行使用时候，均用到该类。
 */
public final class AggregationBuilders {

    public static MaxAggregation.Builder max(String aggregationName, String fieldName) {
        return MaxAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static MinAggregation.Builder min(String aggregationName, String fieldName) {
        return MinAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static SumAggregation.Builder sum(String aggregationName, String fieldName) {
        return SumAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static AvgAggregation.Builder avg(String aggregationName, String fieldName) {
        return AvgAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static DistinctCountAggregation.Builder distinctCount(String aggregationName, String fieldName) {
        return DistinctCountAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static CountAggregation.Builder count(String aggregationName, String fieldName) {
        return CountAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }

    public static TopRowsAggregation.Builder topRows(String aggregationName) {
        return TopRowsAggregation.newBuilder().aggName(aggregationName);
    }

    public static PercentilesAggregation.Builder percentiles(String aggregationName, String fieldName) {
        return PercentilesAggregation.newBuilder().aggName(aggregationName).fieldName(fieldName);
    }
}
