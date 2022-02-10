package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * DescribeDeliveryTask返回的结果
 */
public class DescribeDeliveryTaskResponse extends Response {

    /**
     * 投递任务配置
     */
    private OSSTaskConfig taskConfig;

    /**
     * 投递任务状态
     */
    private TaskSyncStat taskSyncStat;

    /**
     * 投递任务类型
     */
    private DeliveryTaskType taskType;

    public DescribeDeliveryTaskResponse (Response meta) {super(meta);}

    /**
     * 获取投递任务配置
     * @return 投递任务配置信息
     */
    public OSSTaskConfig getTaskConfig() { return taskConfig; }

    /**
     * 内部接口，请勿使用
     */
    public void setTaskConfig(OSSTaskConfig taskConfig) { this.taskConfig = taskConfig; }

    /**
     * 获取任务同步状态信息
     * @return 任务同步状态信息
     */
    public TaskSyncStat getTaskSyncStat() { return taskSyncStat; }

    /**
     * 内部接口，请勿使用
     * @param taskSyncStat
     */
    public void setTaskSyncStat(TaskSyncStat taskSyncStat) { this.taskSyncStat = taskSyncStat; }

    /**
     * 获取投递任务类型
     * @return 投递任务类型
     */
    public DeliveryTaskType getTaskType() { return  taskType; }

    /**
     * 内部接口，请勿使用
     */
    public void setTaskType(DeliveryTaskType taskType) { this.taskType = taskType; }

    /**
     * 投递任务是否已经完成
     * @return 是否完成
     */
    public boolean isFinished() {
        if (taskSyncStat != null && taskType != null) {
            return taskType == DeliveryTaskType.BASE && taskSyncStat.getTaskSyncPhase() == TaskSyncPhase.INCR;
        }
        return false;
    }
}
