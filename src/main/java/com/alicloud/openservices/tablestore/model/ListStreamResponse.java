package com.alicloud.openservices.tablestore.model;

import java.util.List;

public class ListStreamResponse extends Response {

    /**
     * 请求返回的Stream列表
     */
    private List<Stream> streams;

    public ListStreamResponse() {
    }

    public ListStreamResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取Stream列表
     * @return Stream列表
     */
    public List<Stream> getStreams() {
        return streams;
    }

    public void setStreams(List<Stream> streams) {
        this.streams = streams;
    }
}
