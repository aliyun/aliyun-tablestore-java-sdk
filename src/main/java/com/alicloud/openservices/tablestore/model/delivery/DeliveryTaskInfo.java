package com.alicloud.openservices.tablestore.model.delivery;

public class DeliveryTaskInfo {

    /**
     * 表名
     */
    private String tableName;

    /**
     * 任务名
     */
    private String taskName;

    /**
     * 投递任务类型
     */
    private DeliveryTaskType taskType;

    /**
     * 获取表名
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 内部接口，请勿使用
     */
    public void setTableName(String tablename) { this.tableName = tablename; }

    /**
     * 投递获取任务名
     * @return 投递任务名
     */
    public String getTaskName() { return taskName; }

    /**
     * 内部接口，请勿使用
     */
    public void setTaskName(String taskName) { this.taskName = taskName; }

    /**
     * 获取任务类型
     * @return 任务类型
     */
    public DeliveryTaskType getTaskType() { return taskType; }

    /**
     * 内部接口，请勿使用
     */
    public void setTaskType(DeliveryTaskType taskType) { this.taskType = taskType; }
}
