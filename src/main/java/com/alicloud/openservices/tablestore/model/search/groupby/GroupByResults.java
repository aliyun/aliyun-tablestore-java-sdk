package com.alicloud.openservices.tablestore.model.search.groupby;

import java.util.Map;

public class GroupByResults {

    private Map<String, GroupByResult> groupByResultMap;

    public int size(){
        return groupByResultMap.size();
    }

    public GroupByResults setGroupByResultMap(
        Map<String, GroupByResult> groupByResultMap) {
        this.groupByResultMap = groupByResultMap;
        return this;
    }

    public Map<String, GroupByResult> getResultAsMap() {
        return groupByResultMap;
    }

    public GroupByFieldResult getAsGroupByFieldResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_FIELD) {
                return (GroupByFieldResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this groupByName can't cast to GroupByFieldResult.");
            }
        }
    }

    public GroupByGeoDistanceResult getAsGroupByGeoDistanceResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_GEO_DISTANCE) {
                return (GroupByGeoDistanceResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this groupByName can't cast to GroupByGeoDistanceResult.");
            }
        }
    }

    public GroupByFilterResult getAsGroupByFilterResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_FILTER) {
                return (GroupByFilterResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this groupByName can't cast to GroupByFilterResult.");
            }
        }
    }

    public GroupByRangeResult getAsGroupByRangeResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_RANGE) {
                return (GroupByRangeResult)result;
            } else {
                throw new IllegalArgumentException(
                    "the result with this groupByName can't cast to GroupByRangeResult.");
            }
        }
    }

    public GroupByHistogramResult getAsGroupByHistogramResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_HISTOGRAM) {
                return (GroupByHistogramResult) result;
            } else {
                throw new IllegalArgumentException(
                        "the result with this groupByName can't cast to GroupByHistogramResult.");
            }
        }
    }

}
