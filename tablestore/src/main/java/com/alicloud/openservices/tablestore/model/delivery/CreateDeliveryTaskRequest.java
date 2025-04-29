package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class CreateDeliveryTaskRequest implements Request {
    /**
     * Table name
     */
    private String tableName;

    /**
     * Delivery task name
     */
    private String taskName;

    /**
     * Delivery task configuration
     */
    private OSSTaskConfig taskConfig;

    /**
     * Delivery task type, currently supports three types: full, incremental, and full plus incremental.
     */
    private DeliveryTaskType taskType;

    /**
     * Initialize the CreateDeliveryTask instance
     */
    public CreateDeliveryTaskRequest() {}

    /**
     * Initialize the CreateDeliveryTask instance
     * @param tableName The name of the table
     * @param taskName  The name of the task
     * @param taskConfig Delivery task configuration
     */
    public CreateDeliveryTaskRequest(String tableName, String taskName, OSSTaskConfig taskConfig) {
        setTableName(tableName);
        setTaskName(taskName);
        setTaskConfig(taskConfig);
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
     * Get the delivery task name
     *
     * @return Task name
     */
    public String getTaskName() { return taskName; }

    /**
     * Set the delivery task name
     *
     * @param taskName The name of the task
     */
    public void setTaskName(String  taskName) {
        Preconditions.checkArgument(taskName != null && !taskName.isEmpty(), "taskName should not be null or empty");
        this.taskName = taskName;
    }

    /**
     * Get the delivery task configuration
     *
     * @return Task configuration
     */
    public OSSTaskConfig getTaskConfig() { return taskConfig; }

    /**
     * Set the delivery task configuration
     *
     * @param taskConfig Task configuration
     */
    public void setTaskConfig(OSSTaskConfig taskConfig) {
        Preconditions.checkNotNull(taskConfig);
        this.taskConfig = taskConfig;
    }

    /**
     * Get the delivery task type
     *
     * @return Task type
     */
    public DeliveryTaskType getTaskType() { return taskType; }

    /**
     * Set the delivery task type
     *
     * @param taskType The type of delivery task
     */
    public void setTaskType(DeliveryTaskType taskType) {
        Preconditions.checkNotNull(taskType);
        this.taskType = taskType;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_CREATE_DELIVERY_TASK; }
}


