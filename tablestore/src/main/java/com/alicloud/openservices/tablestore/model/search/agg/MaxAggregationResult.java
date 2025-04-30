package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * The result of {@link MaxAggregation}
 */
public class MaxAggregationResult implements AggregationResult {

    private String aggName;

    private double value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_MAX;
    }

    public double getValue() {
        return value;
    }

    public MaxAggregationResult setValue(double value) {
        this.value = value;
        return this;
    }

    public MaxAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }
}
