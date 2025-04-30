package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * The result returned by DescribeDeliveryTask
 */
public class DescribeDeliveryTaskResponse extends Response {

    /**
     * Delivery task configuration
     */
    private OSSTaskConfig taskConfig;

    /**
     * Delivery task status
     */
    private TaskSyncStat taskSyncStat;

    /**
     * Delivery task type
     */
    private DeliveryTaskType taskType;

    public DescribeDeliveryTaskResponse (Response meta) {super(meta);}

    /**
     * Get the delivery task configuration
     * @return Delivery task configuration information
     */
    public OSSTaskConfig getTaskConfig() { return taskConfig; }

    /**
     * Internal interface, do not use
     */
    public void setTaskConfig(OSSTaskConfig taskConfig) { this.taskConfig = taskConfig; }

    /**
     * Get the task synchronization status information
     * @return Task synchronization status information
     */
    public TaskSyncStat getTaskSyncStat() { return taskSyncStat; }

    /**
     * Internal interface, please do not use
     * @param taskSyncStat
     */
    public void setTaskSyncStat(TaskSyncStat taskSyncStat) { this.taskSyncStat = taskSyncStat; }

    /**
     * Get the delivery task type
     * @return Delivery task type
     */
    public DeliveryTaskType getTaskType() { return  taskType; }

    /**
     * Internal interface, do not use
     */
    public void setTaskType(DeliveryTaskType taskType) { this.taskType = taskType; }

    /**
     * Whether the delivery task has been completed
     * @return Completion status
     */
    public boolean isFinished() {
        if (taskSyncStat != null && taskType != null) {
            return taskType == DeliveryTaskType.BASE && taskSyncStat.getTaskSyncPhase() == TaskSyncPhase.INCR;
        }
        return false;
    }
}
