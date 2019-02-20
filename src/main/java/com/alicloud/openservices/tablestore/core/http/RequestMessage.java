package com.alicloud.openservices.tablestore.core.http;

import org.apache.http.client.methods.HttpRequestBase;

public class RequestMessage {

    private HttpRequestBase request;
    private OTSUri actionUri;
    private String contentMd5;
    private int contentLength = 0;

    public RequestMessage(HttpRequestBase request) {
        this.request = request;
    }

    public OTSUri getActionUri() {
        return actionUri;
    }

    public void setActionUri(OTSUri actionUri) {
        this.actionUri = actionUri;
    }

    public HttpRequestBase getRequest() {
        return request;
    }

    public void addHeader(String name, String value) {
        this.request.addHeader(name, value);
    }

    public void setContentMd5(String contentMd5) {
        this.contentMd5 = contentMd5;
    }

    public String getContentMd5() {
        return contentMd5;
    }

    public int getContentLength() {
        return contentLength;
    }

    public void setContentLength(int contentLength) {
        this.contentLength = contentLength;
    }
}
