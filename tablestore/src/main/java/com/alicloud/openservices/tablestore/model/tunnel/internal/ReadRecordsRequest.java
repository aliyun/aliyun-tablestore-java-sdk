package com.alicloud.openservices.tablestore.model.tunnel.internal;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class ReadRecordsRequest implements Request {
    private String tunneId;
    private String clientId;
    private String channelId;
    private String token;

    public ReadRecordsRequest(String tunneId, String clientId, String channelId, String token) {
        this.tunneId = tunneId;
        this.clientId = clientId;
        this.channelId = channelId;
        this.token = token;
    }

    public String getTunneId() {
        return tunneId;
    }

    public void setTunneId(String tunneId) {
        this.tunneId = tunneId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_READRECORDS;
    }
}
