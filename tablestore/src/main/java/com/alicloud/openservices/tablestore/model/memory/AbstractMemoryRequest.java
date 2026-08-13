package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;

public abstract class AbstractMemoryRequest implements MemoryRequest {
    private transient String jsonStr;

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    @Override
    public String toJson() {
        return jsonStr != null ? jsonStr : GsonUtils.toJson(this);
    }
}
