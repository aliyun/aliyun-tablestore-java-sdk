package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class ListDeliveryTaskRequest implements Request {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 初始化ListDeliveryTaskRequest实例
     *
     * @param tableName 表名
     */
    public ListDeliveryTaskRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * 获取表名
     * @return 表名
     */
    public String getTableName() { return tableName; }

    /**
     *设置表名
     * @param tableName 表名
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName should not be null or empty");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_LIST_DELIVERY_TASK; }
}
