package com.alicloud.openservices.tablestore.core.http;

/**
 * 对返回结果进行处理。
 */
public interface ResponseHandler {

    /**
     * 处理返回的结果
     *
     * @param responseData 返回结果
     */
    public void handle(ResponseMessage responseData);
}
