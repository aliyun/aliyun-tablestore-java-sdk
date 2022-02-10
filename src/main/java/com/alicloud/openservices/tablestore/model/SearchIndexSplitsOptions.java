package com.alicloud.openservices.tablestore.model;

/**
 * for {@link ComputeSplitsRequest}
 */
public class SearchIndexSplitsOptions implements SplitsOptions{

    private String indexName;

    public SearchIndexSplitsOptions() {
    }

    public SearchIndexSplitsOptions(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    public SearchIndexSplitsOptions setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }
}
