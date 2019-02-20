package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DeleteIndexRequest implements Request {
    /**
     * 表的名称。
     */
    private String mainTableName;

    /**
     * 索引表的名称。
     */
    private String indexName;
    
    public DeleteIndexRequest(String tableName, String indexName) {
        setMainTableName(tableName);
        setIndeName(indexName);
    }

    /**
     * 获取表的名称。
     * @return 表的名称。
     */
    public String getMainTableName() {
        return mainTableName;
    }


    public void setMainTableName(String tableName) {
        Preconditions.checkArgument(
            tableName != null && !tableName.isEmpty(),
            "The name of table should not be null or empty.");
        this.mainTableName = tableName;
    }

    /**
     * 获取索引表的名称。
     * @return 索引表的名称
     */
    public String getIndexName() { return indexName; }

    public void setIndeName(String indexName) {
        Preconditions.checkArgument(indexName != null && !indexName.isEmpty(),
                "The name of index should not be null or empty");
        this.indexName = indexName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_INDEX;
    }
}
