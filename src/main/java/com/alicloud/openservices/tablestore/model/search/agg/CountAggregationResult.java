package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * {@link CountAggregation}的统计结果
 */
public class CountAggregationResult implements AggregationResult {

    private String aggName;
    private long value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_COUNT;
    }

    public CountAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public long getValue() {
        return value;
    }

    public CountAggregationResult setValue(long value) {
        this.value = value;
        return this;
    }
}
