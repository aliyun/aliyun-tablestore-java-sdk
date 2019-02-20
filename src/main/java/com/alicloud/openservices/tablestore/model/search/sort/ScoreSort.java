package com.alicloud.openservices.tablestore.model.search.sort;

public class ScoreSort implements Sort.Sorter {

    private SortOrder order = SortOrder.DESC;

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }
}
