package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.GlobalTableTypes.GlobalTableStatus;

/**
 * UnbindGlobalTable操作的返回结果。
 */
public class UnbindGlobalTableResponse extends Response {
    private String globalTableId;
    private GlobalTableTypes.GlobalTableStatus status;

    public UnbindGlobalTableResponse(Response meta) {
        super(meta);
    }

    public String getGlobalTableId() {
        return globalTableId;
    }

    public void setGlobalTableId(String globalTableId) {
        this.globalTableId = globalTableId;
    }

    public GlobalTableStatus getStatus() {
        return status;
    }

    public void setStatus(GlobalTableStatus status) {
        this.status = status;
    }
}
