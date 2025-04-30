package com.alicloud.openservices.tablestore.model.delivery;

public class DeliveryTaskInfo {

    /**
     * Table name
     */
    private String tableName;

    /**
     * Task name
     */
    private String taskName;

    /**
     * Delivery task type
     */
    private DeliveryTaskType taskType;

    /**
     * Get the table name
     * @return Table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Internal interface, do not use
     */
    public void setTableName(String tablename) { this.tableName = tablename; }

    /**
     * Deliver and get task name
     * @return Delivery task name
     */
    public String getTaskName() { return taskName; }

    /**
     * Internal interface, do not use
     */
    public void setTaskName(String taskName) { this.taskName = taskName; }

    /**
     * Get the task type
     * @return Task type
     */
    public DeliveryTaskType getTaskType() { return taskType; }

    /**
     * Internal interface, do not use
     */
    public void setTaskType(DeliveryTaskType taskType) { this.taskType = taskType; }
}
