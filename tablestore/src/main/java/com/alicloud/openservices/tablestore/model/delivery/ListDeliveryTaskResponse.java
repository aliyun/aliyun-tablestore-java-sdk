package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * The result returned by ListDeliveryTask
 */
public class ListDeliveryTaskResponse extends Response {

    /**
     * Delivery task list
     */
    private List<DeliveryTaskInfo> taskInfos;

    public ListDeliveryTaskResponse(Response meta) {super(meta);}

    /**
     * Get the delivery task list
     * @return
     */
    public List<DeliveryTaskInfo> getTaskInfos() { return taskInfos; }

    /**
     * Internal interface, do not use
     */
    public void setTaskInfos(List<DeliveryTaskInfo> taskInfos) { this.taskInfos = taskInfos; }
}
