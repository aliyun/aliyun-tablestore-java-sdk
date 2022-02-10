package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.model.search.IndexSchema;

public class SearchInfo
{
    public String getSearchIndexName() {
        return searchIndexName;
    }

    public IndexSchema getSearchSchema() {
        return searchSchema;
    }

    private String searchIndexName;
    private IndexSchema searchSchema;

    public SearchInfo(String name, IndexSchema schema) {
        this.searchIndexName = name;
        this.searchSchema = schema;
    }
}
