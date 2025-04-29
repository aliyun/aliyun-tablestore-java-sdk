package com.alicloud.openservices.tablestore.model.search.agg;

import java.util.List;

/**
 * the result of {@link PercentilesAggregation}
 */
public class PercentilesAggregationResult implements AggregationResult {

    private String aggName;
    private List<PercentilesAggregationItem> percentilesAggregationItems;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_PERCENTILES;
    }

    public PercentilesAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public List<PercentilesAggregationItem> getPercentilesAggregationItems() {
        return percentilesAggregationItems;
    }

    public PercentilesAggregationResult setPercentilesAggregationItems(
        List<PercentilesAggregationItem> percentilesAggregationItems) {
        this.percentilesAggregationItems = percentilesAggregationItems;
        return this;
    }
}
