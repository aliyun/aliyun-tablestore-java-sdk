package com.alicloud.openservices.tablestore.model.search.sort;

public class GroupKeySort {
    private SortOrder order;

    private GroupKeySort(SortOrder order) {
        this.order = order;
    }

    public static GroupKeySort asc() {
        return new GroupKeySort(SortOrder.ASC);
    }

    public static GroupKeySort desc() {
        return new GroupKeySort(SortOrder.DESC);
    }

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public GroupKeySort() {
    }
}
