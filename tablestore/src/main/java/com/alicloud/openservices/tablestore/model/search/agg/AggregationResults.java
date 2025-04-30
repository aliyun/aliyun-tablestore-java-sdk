package com.alicloud.openservices.tablestore.model.search.agg;

import java.util.Map;

/**
 * Used to obtain the results of Aggregation
 */
public class AggregationResults {

    private Map<String, AggregationResult> resultMap;

    public int size() {
        if (resultMap == null) {
            return 0;
        }
        return resultMap.size();
    }

    public AggregationResults setResultMap(
        Map<String, AggregationResult> resultMap) {
        this.resultMap = resultMap;
        return this;
    }

    public Map<String, AggregationResult> getResultAsMap() {
        return resultMap;
    }

    public AvgAggregationResult getAsAvgAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_AVG) {
                return (AvgAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to AvgAggregationResult.");
            }
        }

    }

    public DistinctCountAggregationResult getAsDistinctCountAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_DISTINCT_COUNT) {
                return (DistinctCountAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to DistinctCountAggregationResult.");
            }
        }

    }

    public MaxAggregationResult getAsMaxAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_MAX) {
                return (MaxAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to MaxAggregationResult.");
            }
        }

    }

    public MinAggregationResult getAsMinAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_MIN) {
                return (MinAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to MinAggregationResult.");
            }
        }

    }

    public SumAggregationResult getAsSumAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_SUM) {
                return (SumAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to SumAggregationResult.");
            }
        }

    }

    public CountAggregationResult getAsCountAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_COUNT) {
                return (CountAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to CountAggregationResult.");
            }
        }

    }

    public TopRowsAggregationResult getAsTopRowsAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_TOP_ROWS) {
                return (TopRowsAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to TopRowsAggregationResult.");
            }
        }
    }

    public PercentilesAggregationResult getAsPercentilesAggregationResult(String aggregationName) {
        if (resultMap != null && !resultMap.containsKey(aggregationName)) {
            throw new IllegalArgumentException("AggregationResults don't contains: " + aggregationName);
        } else {
            assert resultMap != null;
            AggregationResult result = resultMap.get(aggregationName);
            if (result.getAggType() == AggregationType.AGG_PERCENTILES) {
                return (PercentilesAggregationResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this aggregationName can't cast to PercentilesAggregationResult.");
            }
        }
    }
}
