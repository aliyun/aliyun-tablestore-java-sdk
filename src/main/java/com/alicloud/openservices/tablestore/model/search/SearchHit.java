package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightResultItem;

import java.util.HashMap;
import java.util.Map;

public class SearchHit {
    private Row row;

    /**
     * 数据行的相关性分数，
     * <p>
     * <b>不计算分数时为NaN</b>
     */
    private Double score;

    /**
     * 嵌套列的子数据行的偏移信息
     * <p>非子文档则值为null</p>
     */
    private Integer nestedDocOffset;

    /**
     * 数据行的高亮片段
     */
    private HighlightResultItem highlightResultItem;

    /**
     * 嵌套子列信息
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
