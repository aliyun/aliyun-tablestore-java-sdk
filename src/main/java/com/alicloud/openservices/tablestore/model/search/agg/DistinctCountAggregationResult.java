package com.alicloud.openservices.tablestore.model.search.agg;
/**
 * {@link DistinctCountAggregation}的统计结果
 */
public class DistinctCountAggregationResult implements AggregationResult {

    private String aggName;
    private long value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_DISTINCT_COUNT;
    }

    public DistinctCountAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public long getValue() {
        return value;
    }

    public DistinctCountAggregationResult setValue(long count) {
        this.value = count;
        return this;
    }
}
