package com.aliyun.openservices.ots.comm;

import org.apache.http.client.methods.HttpRequestBase;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertStringNotNullOrEmpty;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class RequestMessage {

    private HttpRequestBase request;
    private Map<String, String> queryParameters;
    private OTSUri actionUri;
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

    public int getContentLength() {
        return contentLength;
    }

    public void setContentLength(int contentLength) {
        this.contentLength = contentLength;
    }

    public Map<String, String> getQueryParameters() {
        return queryParameters;
    }

    public void setQueryParameters(Map<String, String> queryParameters) {
        this.queryParameters = queryParameters;
    }
}
