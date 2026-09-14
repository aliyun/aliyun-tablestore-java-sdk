package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class GetItemVersionRequest extends ItemRequest {
    private String versionId;
    private String view;

    public GetItemVersionRequest() {
    }

    public GetItemVersionRequest(String memoryStoreName, Scope scope, String versionId) {
        super(memoryStoreName, scope);
        this.versionId = versionId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_ITEM_VERSION;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }
}
