package com.alicloud.openservices.tablestore.model.search;

import java.util.ArrayList;
import java.util.List;

/**
 * 嵌套列的数据行
 */
public class SearchInnerHit {
    /**
     * 嵌套列的Path目录
     */
    private String path;

    /**
     * 指定path下的所有数据子行的内容
     */
    private List<SearchHit> subSearchHits = new ArrayList<SearchHit>();

    public SearchInnerHit() {
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    public void addSubSearchHit(SearchHit searchHit) {
        this.subSearchHits.add(searchHit);
    }

    public void setSubSearchHits(List<SearchHit> searchHits) {
        this.subSearchHits = searchHits;
    }

    public List<SearchHit> getSubSearchHits() {
        return this.subSearchHits;
    }
}
