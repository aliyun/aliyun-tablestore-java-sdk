package com.alicloud.openservices.tablestore.model.memory;

public class MemoryStoreListItem {
    private String memoryStoreName;

    public MemoryStoreListItem() {
    }

    public MemoryStoreListItem(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }

    public String getMemoryStoreName() {
        return memoryStoreName;
    }

    public void setMemoryStoreName(String memoryStoreName) {
        this.memoryStoreName = memoryStoreName;
    }
}
