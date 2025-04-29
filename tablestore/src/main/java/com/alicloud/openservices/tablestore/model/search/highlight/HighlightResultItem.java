package com.alicloud.openservices.tablestore.model.search.highlight;

import java.util.HashMap;
import java.util.Map;

/**
 * In a single row of data, the results of all query highlighted fields
 */
public class HighlightResultItem {
    private Map<String, HighlightField> highlightFields = new HashMap<String, HighlightField>();

    public Map<String, HighlightField> getHighlightFields() {
        return highlightFields;
    }

    public void setHighlightFields(Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public void addHighlightField(String fieldName, HighlightField highlightField) {
        this.highlightFields.put(fieldName, highlightField);
    }

    /**
     * Get the highlighted result of the field by its field name. If there is no highlighted result, the return value will be {@code null}.
     *
     * @param fieldName
     * @return HighlightField
     */
    public HighlightField getHighlightFieldByName(String fieldName) {
        return this.highlightFields.get(fieldName);
    }
}
