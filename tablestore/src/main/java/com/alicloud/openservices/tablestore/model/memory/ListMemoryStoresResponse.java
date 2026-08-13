package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListMemoryStoresResponse extends Response {
    private List<MemoryStoreListItem> stores;
    private String nextToken;

    public ListMemoryStoresResponse() {
    }

    public List<MemoryStoreListItem> getStores() {
        return stores;
    }

    public void setStores(List<MemoryStoreListItem> stores) {
        this.stores = stores;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
