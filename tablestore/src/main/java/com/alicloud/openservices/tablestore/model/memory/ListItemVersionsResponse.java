package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.model.Response;
import java.util.List;

public class ListItemVersionsResponse extends Response {
    private String type;
    private List<ItemVersion> versions;
    private String nextToken;

    public ListItemVersionsResponse() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<ItemVersion> getVersions() {
        return versions;
    }

    public void setVersions(List<ItemVersion> versions) {
        this.versions = versions;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}
