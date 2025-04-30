package com.alicloud.openservices.tablestore.model.search.sort;

public class RowCountSort {
    private SortOrder order;

    private RowCountSort(SortOrder order) {
        this.order = order;
    }

    public SortOrder getOrder() {
        return order;
    }

    public static RowCountSort asc() {
        return new RowCountSort(SortOrder.ASC);
    }

    public static RowCountSort desc() {
        return new RowCountSort(SortOrder.DESC);
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public RowCountSort() {
    }
}
