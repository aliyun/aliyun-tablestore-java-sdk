package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.OperationNames;

public class UpdateItemRequest extends ItemRequest {
    private String itemId;
    private String content;
    private String path;
    private ItemPrecondition precondition;
    private Boolean overwrite;
    private String sessionId;
    private String view;

    public UpdateItemRequest() {
    }

    public UpdateItemRequest(String memoryStoreName, Scope scope, String itemId) {
        super(memoryStoreName, scope);
        this.itemId = itemId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_ITEM;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public ItemPrecondition getPrecondition() {
        return precondition;
    }

    public void setPrecondition(ItemPrecondition precondition) {
        this.precondition = precondition;
    }

    public Boolean getOverwrite() {
        return overwrite;
    }

    public void setOverwrite(Boolean overwrite) {
        this.overwrite = overwrite;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
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
