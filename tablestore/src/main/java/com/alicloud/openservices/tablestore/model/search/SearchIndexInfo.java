package com.alicloud.openservices.tablestore.model.search;

public class SearchIndexInfo {

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


    public String toString() {
        return "{TableName:" + tableName + ",IndexName:" + indexName + "}";
    }
}
