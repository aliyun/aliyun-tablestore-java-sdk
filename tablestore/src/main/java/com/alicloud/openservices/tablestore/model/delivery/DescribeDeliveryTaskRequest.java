package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class DescribeDeliveryTaskRequest implements Request {

    /**
     * Table name
     */
    private String tableName;

    /**
     * Task name
     */
    private String taskName;

    /**
     * Initialize the DescribeDeliveryTaskRequest instance
     *
     * @param tableName The name of the table
     * @param taskName  The name of the task
     */
    public DescribeDeliveryTaskRequest(String tableName, String taskName) {
        setTableName(tableName);
        setTaskName(taskName);
    }

    /**
     * Get the table name
     *
     * @return table name
     */
    public String getTableName() { return tableName; }

    /**
     * Set the table name
     *
     * @param tableName The name of the table
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName should not be null or empty");
        this.tableName = tableName;
    }

    /**
     * Get the task name
     *
     * @return Task name
     */
    public String getTaskName() { return taskName; }

    /**
     * Set the task name
     *
     * @param taskName The name of the task
     */
    public void setTaskName(String  taskName) {
        Preconditions.checkArgument(taskName != null && !taskName.isEmpty(), "taskName should not be null or empty");
        this.taskName = taskName;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_DESCRIBE_DELIVERY_TASK; }
}
