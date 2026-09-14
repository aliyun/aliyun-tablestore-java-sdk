package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * Acknowledgement of a {@link DeleteItemVersionsRequest}. The deletion runs asynchronously, so a successful
 * response only means the task was accepted.
 */
public class DeleteItemVersionsResponse extends Response {
    private String type;
    private String taskId;
    private String status;

    public DeleteItemVersionsResponse() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * Identifier of the asynchronous deletion task, useful for diagnostics. It cannot be passed to
     * GetMemoryTask; poll ListItemVersions on the same scope to decide whether the cleanup finished.
     */
    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    /**
     * {@code pending} when the task is accepted, which does not mean the versions are already deleted.
     */
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
