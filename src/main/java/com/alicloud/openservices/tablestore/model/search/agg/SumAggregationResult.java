package com.alicloud.openservices.tablestore.model.search.agg;
/**
 * {@link SumAggregation}的统计结果
 */
public class SumAggregationResult implements AggregationResult {

    private String aggName;
    private double value;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_SUM;
    }

    public double getValue() {
        return value;
    }

    public SumAggregationResult setValue(double value) {
        this.value = value;
        return this;
    }

    public SumAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }
}
