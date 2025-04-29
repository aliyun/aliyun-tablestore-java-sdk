package com.alicloud.openservices.tablestore.model.search;

/**
 * Field collapse, which can achieve deduplication of results for a specific field.
 * <p>Use case example:</p>
 * <p>In an app-based food ordering scenario, if I want to order the most popular dishes from the eight major Chinese cuisines. Using traditional methods, we might need to query the most popular dish for each of the 8 cuisine types separately.
 * However, by setting {@link Collapse} to the cuisine type, we can return the 8 hottest dishes (one from each cuisine, as {@link Collapse} helps us eliminate duplicates). This fulfills the user's needs with just one query.</p>
 */
public class Collapse {
    public Collapse(String fieldName) {
        this.fieldName = fieldName;
    }

    private String fieldName;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
