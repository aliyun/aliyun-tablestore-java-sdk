package com.alicloud.openservices.tablestore.model.tunnel.internal;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;

public class HeartbeatResponse extends Response {
    private List<Channel> channels;

    public HeartbeatResponse(Response meta) {
        super(meta);
    }

    public List<Channel> getChannels() {
        return channels;
    }

    public void setChannels(List<Channel> channels) {
        this.channels = channels;
    }
}
