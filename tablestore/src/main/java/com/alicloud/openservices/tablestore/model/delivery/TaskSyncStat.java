package com.alicloud.openservices.tablestore.model.delivery;

import java.util.Date;

public class TaskSyncStat {

    /**
     * Delivery task status
     */
    private TaskSyncPhase taskSyncPhase;

    /**
     * Current synchronization time point
     */
    private Date currentSyncPoint;

    /**
     * Error type
     */
    private DeliveryErrorType errorType;

    /**
     * More detailed information
     */
    private String detail;

    /**
     * Get the task delivery status
     * @return
     */
    public TaskSyncPhase getTaskSyncPhase() { return taskSyncPhase; }

    /**
     * Internal interface, please do not use
     */
    public void setTaskSyncPhase(TaskSyncPhase taskSyncPhase) { this.taskSyncPhase = taskSyncPhase; }

    /**
     * Get the current sync timestamp
     * @return
     */
    public Date getCurrentSyncPoint() { return currentSyncPoint; }

    /**
     * Internal interface, do not use
     */
    public void setCurrentSyncPoint(Date currentSyncPoint) { this.currentSyncPoint = currentSyncPoint; }

    /**
     * Get the error type
     *
     * @return error type
     */
    public DeliveryErrorType getErrorType() { return errorType; }

    /**
     * Internal interface, do not use
     */
    public void setErrorType(DeliveryErrorType errorType) { this.errorType = errorType; }

    /**
     * Get more detailed information
     * @return
     */
    public String getDetail() { return detail; }

    /**
     * Internal interface, do not use
     */
    public void setDetail(String detail) { this.detail = detail; }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("taskSyncPhase: ").append(taskSyncPhase).append(", current_sync_timestamp: ").append(currentSyncPoint.toString())
                .append(", ErrorType: ").append(errorType).append(", detail: ").append(detail).append("}");
        return sb.toString();
    }
}
