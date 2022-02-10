package com.alicloud.openservices.tablestore.model.delivery;

import java.util.Date;

public class TaskSyncStat {

    /**
     * 投递任务状态
     */
    private TaskSyncPhase taskSyncPhase;

    /**
     * 当前同步时间时间点
     */
    private Date currentSyncPoint;

    /**
     * 错误类型
     */
    private DeliveryErrorType errorType;

    /**
     * 更多详细信息
     */
    private String detail;

    /**
     * 获取任务投递状态
     * @return
     */
    public TaskSyncPhase getTaskSyncPhase() { return taskSyncPhase; }

    /**
     *  内部接口，请勿使用
     */
    public void setTaskSyncPhase(TaskSyncPhase taskSyncPhase) { this.taskSyncPhase = taskSyncPhase; }

    /**
     * 获取当前同步时间戳
     * @return
     */
    public Date getCurrentSyncPoint() { return currentSyncPoint; }

    /**
     * 内部接口，请勿使用
     */
    public void setCurrentSyncPoint(Date currentSyncPoint) { this.currentSyncPoint = currentSyncPoint; }

    /**
     * 获取错误类型
     *
     * @return 错误类型
     */
    public DeliveryErrorType getErrorType() { return errorType; }

    /**
     * 内部接口，请勿使用
     */
    public void setErrorType(DeliveryErrorType errorType) { this.errorType = errorType; }

    /**
     * 获取更多详细信息
     * @return
     */
    public String getDetail() { return detail; }

    /**
     * 内部接口，请勿使用
     */
    public void setDetail(String detail) { this.detail = detail; }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("taskSyncPhase: ").append(taskSyncPhase).append(", current_sync_timestamp: ").append(currentSyncPoint.toString())
                .append(", ErrorType: ").append(errorType).append(", detail: ").append(detail).append("}");
        return sb.toString();
    }
}
