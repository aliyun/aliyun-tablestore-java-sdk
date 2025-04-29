package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightResultItem;

import java.util.HashMap;
import java.util.Map;

public class SearchHit {
    private Row row;

    /**
     * Relevance score of the data row.
     * <p>
     * <b>NaN when the score is not calculated</b>
     */
    private Double score;

    /**
     * Offset information for the child data rows of nested columns.
     * <p>If it is not a sub-document, the value is null.</p>
     */
    private Integer nestedDocOffset;

    /**
     * Highlighted fragment of the data row
     */
    private HighlightResultItem highlightResultItem;

    /**
     * Nested sub-column information
     */
    private final Map<String, SearchInnerHit> searchInnerHits = new HashMap<String, SearchInnerHit>();

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    public HighlightResultItem getHighlightResultItem() {
        return highlightResultItem;
    }

    public void setHighlightResultItem(HighlightResultItem highlightResultItem) {
        this.highlightResultItem = highlightResultItem;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Double getScore() {
        return this.score;
    }

    public void setOffset(Integer offset) {
        this.nestedDocOffset = offset;
    }

    public Integer getOffset() {
        return this.nestedDocOffset;
    }

    public void addSearchInnerHit(String path, SearchInnerHit searchInnerHit) {
        this.searchInnerHits.put(path, searchInnerHit);
    }

    public Map<String, SearchInnerHit> getSearchInnerHits() {
        return this.searchInnerHits;
    }

    public SearchInnerHit getSearchInnerHitByPath(String path) {
        return this.searchInnerHits.get(path);
    }
}
