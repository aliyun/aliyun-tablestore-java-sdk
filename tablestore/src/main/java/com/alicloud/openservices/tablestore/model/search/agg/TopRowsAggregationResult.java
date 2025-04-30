package com.alicloud.openservices.tablestore.model.search.agg;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Row;

/**
 * the result of {@link TopRowsAggregation}
 */
public class TopRowsAggregationResult implements AggregationResult {

    private String aggName;
    private List<Row> rows;

    @Override
    public String getAggName() {
        return aggName;
    }

    @Override
    public AggregationType getAggType() {
        return AggregationType.AGG_TOP_ROWS;
    }

    public TopRowsAggregationResult setAggName(String aggName) {
        this.aggName = aggName;
        return this;
    }

    public List<Row> getRows() {
        return rows;
    }

    public TopRowsAggregationResult setRows(List<Row> rows) {
        this.rows = rows;
        return this;
    }
}
