package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.ots.thirdparty.org.apache.http.Header;
import com.aliyun.ots.thirdparty.org.apache.http.HttpResponse;

public class ResponseMessage {
    private HttpResponse response;
    private Map<String, String> lowerCaseHeaders;

    private static final int HTTP_SUCCESS_STATUS_CODE = 200;

    public ResponseMessage(HttpResponse response) {
        this.response = response;
    }

    public int getStatusCode() {
        return response.getStatusLine().getStatusCode();
    }

    public String getHeader(String header) {
        initHeaderMap();

        return lowerCaseHeaders.get(header.toLowerCase());
    }

    public Map<String, String> getLowerCaseHeadersMap() {
        initHeaderMap();
        return lowerCaseHeaders;
    }

    public HttpResponse getResponse() {
        return response;
    }

    private void initHeaderMap() {
        if (lowerCaseHeaders == null) {
            lowerCaseHeaders = new HashMap<String, String>(response.getAllHeaders().length);
            for (Header header : response.getAllHeaders()) {
                lowerCaseHeaders.put(header.getName().toLowerCase(), header.getValue());
            }
        }
    }

    /**
     * If the return status code is 2XX, it represents success.
     * @return If the return status code is 2XX, return true; otherwise, return false.
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
