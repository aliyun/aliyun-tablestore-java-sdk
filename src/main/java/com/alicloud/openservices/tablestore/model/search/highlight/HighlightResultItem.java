package com.alicloud.openservices.tablestore.model.search.highlight;

import java.util.HashMap;
import java.util.Map;

/**
 * 单行数据中，所有查询高亮字段的结果
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
     * 通过字段名获取该字段高亮结果。若无高亮结果，则返回值为{@code null}
     *
     * @param fieldName
     * @return HighlightField
     */
    public HighlightField getHighlightFieldByName(String fieldName) {
        return this.highlightFields.get(fieldName);
    }
}
