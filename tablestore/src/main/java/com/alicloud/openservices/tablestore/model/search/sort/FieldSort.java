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
     * When some rows of the sorting field have no filled values, the sorting behavior supports the following three options:
     * <p> 1. {@link FieldSort#FIRST_WHEN_MISSING}: When the sorting field value is missing, it is placed at the front.</p>
     * <p> 2. {@link FieldSort#LAST_WHEN_MISSING}: When the sorting field value is missing, it is placed at the back.</p>
     * <p> 3. Custom value: When the sorting field value is missing, the specified value is used for sorting.</p>
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
