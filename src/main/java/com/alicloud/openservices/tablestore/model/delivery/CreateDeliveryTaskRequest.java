package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class CreateDeliveryTaskRequest implements Request {
    /**
     *表名
     */
    private String tableName;

    /**
     *投递任务名
     */
    private String taskName;

    /**
     * 投递任务配置
     */
    private OSSTaskConfig taskConfig;

    /**
     * 投递任务类型，目前支持全量、增量、全量加增量三种类型
     */
    private DeliveryTaskType taskType;

    /**
     * 初始化CreateDeliveryTask实例
     */
    public CreateDeliveryTaskRequest() {}

    /**
     * 初始化CreateDeliveryTask实例
     * @param tableName 表名
     * @param taskName  任务名
     * @param taskConfig 投递任务配置
     */
    public CreateDeliveryTaskRequest(String tableName, String taskName, OSSTaskConfig taskConfig) {
        setTableName(tableName);
        setTaskName(taskName);
        setTaskConfig(taskConfig);
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() { return tableName; }

    /**
     *设置表名
     *
     * @param tableName 表名
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName should not be null or empty");
        this.tableName = tableName;
    }

    /**
     * 获取投递任务名
     *
     * @return 任务名
     */
    public String getTaskName() { return taskName; }

    /**
     * 设置投递任务名
     *
     * @param taskName 任务名
     */
    public void setTaskName(String  taskName) {
        Preconditions.checkArgument(taskName != null && !taskName.isEmpty(), "taskName should not be null or empty");
        this.taskName = taskName;
    }

    /**
     * 获取投递任务配置
     *
     * @return 任务配置
     */
    public OSSTaskConfig getTaskConfig() { return taskConfig; }

    /**
     * 设置投递任务配置
     *
     * @param taskConfig 任务配置
     */
    public void setTaskConfig(OSSTaskConfig taskConfig) {
        Preconditions.checkNotNull(taskConfig);
        this.taskConfig = taskConfig;
    }

    /**
     * 获取投递任务类型
     *
     * @return 任务类型
     */
    public DeliveryTaskType getTaskType() { return taskType; }

    /**
     * 设置投递任务类型
     *
     * @param taskType 投递任务类型
     */
    public void setTaskType(DeliveryTaskType taskType) {
        Preconditions.checkNotNull(taskType);
        this.taskType = taskType;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_CREATE_DELIVERY_TASK; }
}


