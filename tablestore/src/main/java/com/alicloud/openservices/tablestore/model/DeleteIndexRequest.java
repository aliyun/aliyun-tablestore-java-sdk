package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DeleteIndexRequest implements Request {
    /**
     * The name of the table.
     */
    private String mainTableName;

    /**
     * The name of the index table.
     */
    private String indexName;
    
    public DeleteIndexRequest(String tableName, String indexName) {
        setMainTableName(tableName);
        setIndeName(indexName);
    }

    /**
     * Get the name of the table.
     * @return The name of the table.
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
     * Get the name of the index table.
     * @return the name of the index table
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
