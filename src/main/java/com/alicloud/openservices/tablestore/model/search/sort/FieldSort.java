package com.alicloud.openservices.tablestore.model.search.sort;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public class FieldSort implements Sort.Sorter {

    public static final ColumnValue FIRST_WHEN_MISSING = ColumnValue.fromString("_first");
    public static final ColumnValue LAST_WHEN_MISSING = ColumnValue.fromString("_last");

    private String fieldName;
    private SortOrder order = SortOrder.ASC;
    private SortMode mode;
    private NestedFilter nestedFilter;

    /**
     * 当排序的字段某些行没有填充值的时候，排序行为支持以下三种：
     * <p> 1. {@link FieldSort#FIRST_WHEN_MISSING}: 当排序字段值缺省时候排在最前面</p>
     * <p> 2. {@link FieldSort#LAST_WHEN_MISSING}: 当排序字段值缺省时候排在最后面</p>
     * <p> 3. 自定义值: 当排序字段值缺省时候使用指定的值进行排序</p>
     */
    private ColumnValue missingValue;
    
    private String missingField;

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

    @Deprecated
    public ColumnValue getMissing() {
        return missingValue;
    }

    /**
     * @see FieldSort#missingValue
     */
    @Deprecated
    public void setMissing(ColumnValue missing) {
        this.missingValue = missing;
    }

    public ColumnValue getMissingValue() {
        return missingValue;
    }

    public void setMissingValue(ColumnValue missingValue) {
        this.missingValue = missingValue;
    }

    public String getMissingField() {
        return missingField;
    }

    public void setMissingField(String missingField) {
        this.missingField = missingField;
    }
}
