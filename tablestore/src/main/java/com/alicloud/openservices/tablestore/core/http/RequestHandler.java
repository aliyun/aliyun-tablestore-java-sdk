package com.alicloud.openservices.tablestore.core.http;

/**
 * Preprocess the request data that is about to be sent
 */
public interface RequestHandler {

    /**
     * Preprocess the request data to be sent
     */
    public void handle(RequestMessage message);
}
