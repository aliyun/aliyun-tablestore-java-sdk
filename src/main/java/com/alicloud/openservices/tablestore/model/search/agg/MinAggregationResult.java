package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * {@link MinAggregation}的统计结果
 */
public class MinAggregationResult implements AggregationResult {

    private String aggName;
    private double value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_MIN;
    }

    public double getValue() {
        return value;
    }

    public MinAggregationResult setValue(double value) {
        this.value = value;
        return this;
    }

    public MinAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }
}
