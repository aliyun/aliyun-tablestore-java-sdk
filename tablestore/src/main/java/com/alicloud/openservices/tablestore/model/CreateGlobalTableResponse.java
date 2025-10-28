package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.GlobalTableTypes.GlobalTableStatus;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.ServeMode;

/**
 * CreateGlobalTable操作的返回结果。
 */
public class CreateGlobalTableResponse extends Response {
    private String globalTableId;
    private GlobalTableTypes.GlobalTableStatus status;
    private ServeMode serveMode;

    public CreateGlobalTableResponse(Response meta) {
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

    public ServeMode getServeMode() {
        return serveMode;
    }

    public void setServeMode(ServeMode serveMode) {
        this.serveMode = serveMode;
    }
}
