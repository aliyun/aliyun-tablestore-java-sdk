package com.alicloud.openservices.tablestore.model.search.sort;

public class SubAggSort {
    private SortOrder order;
    private String subAggName;

    private SubAggSort(SortOrder order, String subAggName) {
        this.order = order;
        this.subAggName = subAggName;
    }

    public SortOrder getOrder() {
        return order;
    }

    public String getSubAggName() {
        return subAggName;
    }

    public static SubAggSort asc(String subAggName) {
        return new SubAggSort(SortOrder.ASC, subAggName);
    }

    public static SubAggSort desc(String subAggName) {
        return new SubAggSort(SortOrder.DESC, subAggName);
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public void setSubAggName(String subAggName) {
        this.subAggName = subAggName;
    }

    public SubAggSort() {
    }
}
