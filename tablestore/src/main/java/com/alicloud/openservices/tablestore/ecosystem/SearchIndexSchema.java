package com.alicloud.openservices.tablestore.ecosystem;

import java.io.Serializable;
import java.util.List;

public class SearchIndexSchema implements Serializable {

    private List<SearchIndexFieldSchema> schema;

    public SearchIndexSchema(List<SearchIndexFieldSchema> schema) {
        this.schema = schema;
    }
}
