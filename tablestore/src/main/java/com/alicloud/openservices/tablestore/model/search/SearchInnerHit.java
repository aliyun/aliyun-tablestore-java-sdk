package com.alicloud.openservices.tablestore.model.search;

import java.util.ArrayList;
import java.util.List;

/**
 * Data row of nested columns
 */
public class SearchInnerHit {
    /**
     * Path directory for nested columns
     */
    private String path;

    /**
     * The content of all data sub-rows under the specified path
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
