package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * Statistical result of {@link MinAggregation}
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
