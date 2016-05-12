/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.comm;

import org.apache.http.Header;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 表示返回结果的信息。
 *
 */
public class ResponseMessage {
    private HttpResponse response;
    private Map<String, String> headers;

    private static final int HTTP_SUCCESS_STATUS_CODE = 200;

    public ResponseMessage(HttpResponse response) {
        this.response = response;
    }

    public int getStatusCode() {
        return response.getStatusLine().getStatusCode();
    }

    public String getHeader(String header) {
        initHeaderMap();

        return headers.get(header);
    }

    public Map<String, String> getHeadersMap() {
        initHeaderMap();
        return headers;
    }

    public HttpResponse getResponse() {
        return response;
    }

    private void initHeaderMap() {
        if (headers == null) {
            headers = new HashMap<String, String>(response.getAllHeaders().length);
            for (Header header : response.getAllHeaders()) {
                headers.put(header.getName(), header.getValue());
            }
        }
    }

    /**
     * 若返回状态码为2XX，则代表成功。
     * @return 若返回状态码为2XX，则返回true，否则返回false
     */
    public boolean isSuccessful(){
        return (getStatusCode() / 100) == (HTTP_SUCCESS_STATUS_CODE / 100);
    }

    public InputStream getContent() throws IOException {
        return response.getEntity().getContent();
    }

    public void close() throws IOException {
        response.getEntity().getContent().close();
    }
}