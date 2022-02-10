package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * ListDeliveryTask返回的结果
 */
public class ListDeliveryTaskResponse extends Response {

    /**
     * 投递任务列表
     */
    private List<DeliveryTaskInfo> taskInfos;

    public ListDeliveryTaskResponse(Response meta) {super(meta);}

    /**
     * 获取投递任务列表
     * @return
     */
    public List<DeliveryTaskInfo> getTaskInfos() { return taskInfos; }

    /**
     * 内部接口，请勿使用
     */
    public void setTaskInfos(List<DeliveryTaskInfo> taskInfos) { this.taskInfos = taskInfos; }
}
