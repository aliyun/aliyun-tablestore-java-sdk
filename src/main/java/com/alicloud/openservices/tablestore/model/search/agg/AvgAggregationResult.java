package com.alicloud.openservices.tablestore.model.search.agg;

/**
 *  * {@link AvgAggregation}的结果
 */
public class AvgAggregationResult implements AggregationResult {

    /**
     * 聚合的名字
     */
    private String aggName;
    /**
     * 聚合的结果
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
