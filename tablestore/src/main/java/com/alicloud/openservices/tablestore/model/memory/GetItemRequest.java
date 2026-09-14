package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class GetItemRequest extends ItemRequest {
    private String itemId;
    private String path;
    private String view;

    public GetItemRequest() {
    }

    public GetItemRequest(String memoryStoreName, Scope scope) {
        super(memoryStoreName, scope);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_ITEM;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getView() {
        return view;
    }

    public void setView(String view) {
        this.view = view;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
