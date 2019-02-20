package com.alicloud.openservices.tablestore.core.http;

/**
 * 对即将发送的请求数据进行预处理
 */
public interface RequestHandler {

    /**
     * 预处理需要发送的请求数据
     */
    public void handle(RequestMessage message);
}
