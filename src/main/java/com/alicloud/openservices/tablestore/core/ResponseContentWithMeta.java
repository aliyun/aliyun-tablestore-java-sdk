package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.model.Response;
import com.google.protobuf.Message;

public class ResponseContentWithMeta {
    Message message;
    Response meta;

    public ResponseContentWithMeta(Message message, Response meta) {
        this.message = message;
        this.meta = meta;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public Response getMeta() {
        return meta;
    }

    public void setMeta(Response meta) {
        this.meta = meta;
    }
}
