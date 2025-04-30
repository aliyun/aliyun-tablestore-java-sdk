package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DescribeSearchIndexRequest implements Request {

    private String tableName;
    private String indexName;
    private Boolean includeSyncStat = true;

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

    public Boolean isIncludeSyncStat() {
        return includeSyncStat;
    }

    public void setIncludeSyncStat(boolean includeSyncStat) {
        this.includeSyncStat = includeSyncStat;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_SEARCH_INDEX;
    }
}
