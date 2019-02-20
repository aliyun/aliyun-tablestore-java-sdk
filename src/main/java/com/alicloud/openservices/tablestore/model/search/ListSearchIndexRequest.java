package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class ListSearchIndexRequest implements Request {

    /**
     * TableStore的表名字
     */
    private String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_SEARCH_INDEX;
    }
}
