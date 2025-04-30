package com.alicloud.openservices.tablestore.core.http;

/**
 * Process the returned results.
 */
public interface ResponseHandler {

    /**
     * Process the returned result
     *
     * @param responseData The returned result
     */
    public void handle(ResponseMessage responseData);
}
