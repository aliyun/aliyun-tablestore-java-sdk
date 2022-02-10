package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DeleteDeliveryTaskRequest implements Request {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 任务名
     */
    private String taskName;

    /**
     * 初始化DeleteDeliveryTaskRquest实例
     *
     * @param tableName 表名
     * @param taskName  任务名
     */
    public DeleteDeliveryTaskRequest(String tableName, String taskName) {
        setTableName(tableName);
        setTaskName(taskName);
    }

    /**
     * 获取表名
     * @return 表名
     */
    public String getTableName() { return tableName; }

    /**
     * 设置表名
     * @param tableName 表名
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName should not be null or empty");
        this.tableName = tableName;
    }

    /**
     * 获取任务名
     * @return 任务名
     */
    public String getTaskName() { return taskName; }

    /**
     * 设置任务名
     * @param taskName 任务名
     */
    public void setTaskName(String  taskName) {
        Preconditions.checkArgument(taskName != null && !taskName.isEmpty(), "taskName should not be null or empty");
        this.taskName = taskName;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_DELETE_DELIVERY_TASK; }
}


