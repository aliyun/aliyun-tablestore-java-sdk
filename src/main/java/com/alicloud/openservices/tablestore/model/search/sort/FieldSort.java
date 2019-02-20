package com.alicloud.openservices.tablestore.model.search.sort;

public class FieldSort implements Sort.Sorter {

    private String fieldName;
    private SortOrder order = SortOrder.ASC;
    private SortMode mode;
    private NestedFilter nestedFilter;

    public FieldSort(String fieldName) {
        this.fieldName = fieldName;
    }

    public FieldSort(String fieldName, SortOrder order) {
        this.fieldName = fieldName;
        this.order = order;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public SortMode getMode() {
        return mode;
    }

    public void setMode(SortMode mode) {
        this.mode = mode;
    }

    public NestedFilter getNestedFilter() {
        return nestedFilter;
    }

    public void setNestedFilter(NestedFilter nestedFilter) {
        this.nestedFilter = nestedFilter;
    }
}
