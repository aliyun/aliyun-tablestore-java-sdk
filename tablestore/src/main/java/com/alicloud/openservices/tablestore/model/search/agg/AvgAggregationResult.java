package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * The result of {@link AvgAggregation}
 */
public class AvgAggregationResult implements AggregationResult {

    /**
     * The name of the aggregation
     */
    private String aggName;
    /**
     * The result of aggregation
     */
    private double value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_AVG;
    }

    public double getValue() {
        return value;
    }

    public AvgAggregationResult setValue(double value) {
        this.value = value;
        return this;
    }

    public AvgAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }
}
