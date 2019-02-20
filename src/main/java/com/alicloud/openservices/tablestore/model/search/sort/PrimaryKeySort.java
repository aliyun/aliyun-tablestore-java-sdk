package com.alicloud.openservices.tablestore.model.search.sort;

public class PrimaryKeySort implements Sort.Sorter {

    private SortOrder order = SortOrder.ASC;

    public PrimaryKeySort() {
    }

    public PrimaryKeySort(SortOrder order) {
        this.order = order;
    }

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }
}
