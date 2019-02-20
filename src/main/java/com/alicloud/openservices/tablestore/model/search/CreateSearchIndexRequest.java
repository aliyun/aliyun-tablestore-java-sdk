package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class CreateSearchIndexRequest implements Request {

    /**
     * TableStore中的表名称
     */
    private String tableName;
    /**
     * SearchIndex的名称。
     */
    private String indexName;

    /**
     * SearchIndex的schema结构
     */
    private IndexSchema indexSchema;

    public CreateSearchIndexRequest() {
    }

    public CreateSearchIndexRequest(String tableName, String indexName) {
        this.tableName = tableName;
        this.indexName = indexName;
    }

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

    public IndexSchema getIndexSchema() {
        return indexSchema;
    }

    public void setIndexSchema(IndexSchema indexSchema) {
        this.indexSchema = indexSchema;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_SEARCH_INDEX;
    }
}
