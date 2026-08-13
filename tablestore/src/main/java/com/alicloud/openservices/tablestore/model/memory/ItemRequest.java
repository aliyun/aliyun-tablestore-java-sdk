package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.google.gson.JsonObject;

public abstract class ItemRequest extends AbstractMemoryRequest {
    private String type;
    private String memoryStoreName;
    private Scope scope;

    protected ItemRequest() {
    }

    protected ItemRequest(String memoryStoreName, Scope scope) {
        this.memoryStoreName = memoryStoreName;
        this.scope = scope;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    @Override
    public String toJson() {
        if (getJsonStr() != null) {
            return getJsonStr();
        }
        JsonObject json = GsonUtils.getGson().toJsonTree(this).getAsJsonObject();
        if (type == null || type.isEmpty()) {
            json.addProperty("type", MemoryConstants.ITEM_TYPE_MEMORY_FILE);
        }
        return json.toString();
    }
}
