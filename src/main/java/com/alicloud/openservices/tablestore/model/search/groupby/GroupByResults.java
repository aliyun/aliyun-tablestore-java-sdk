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

    public GroupByCompositeResult getAsGroupByCompositeResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_COMPOSITE) {
                return (GroupByCompositeResult) result;
            } else {
                throw new IllegalArgumentException(
                        "the result with this groupByName can't cast to GroupByCompositeResult.");
            }
        }
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

    public GroupByDateHistogramResult getAsGroupByDateHistogramResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_DATE_HISTOGRAM) {
                return (GroupByDateHistogramResult) result;
            } else {
                throw new IllegalArgumentException(
                        "the result with this groupByName can't cast to GroupByDateHistogramResult.");
            }
        }
    }

    public GroupByGeoGridResult getAsGroupByGeoGridResult(String groupByName) {
        if (groupByResultMap != null && !groupByResultMap.containsKey(groupByName)) {
            throw new IllegalArgumentException("GroupByResults don't contains: " + groupByName);
        } else {
            assert groupByResultMap != null;
            GroupByResult result = groupByResultMap.get(groupByName);
            if (result.getGroupByType() == GroupByType.GROUP_BY_GEO_GRID) {
                return (GroupByGeoGridResult) result;
            } else {
                throw new IllegalArgumentException(
                        "the result with this groupByName can't cast to GroupByGeoGridResult.");
            }
        }
    }

}
