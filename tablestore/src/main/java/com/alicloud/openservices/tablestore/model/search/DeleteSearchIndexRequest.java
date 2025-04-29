package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DeleteSearchIndexRequest implements Request {

    private String tableName;
    private String indexName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_SEARCH_INDEX;
    }
}
