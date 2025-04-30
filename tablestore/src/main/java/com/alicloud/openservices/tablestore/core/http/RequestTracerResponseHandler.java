package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.RequestTracer;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RequestTracerResponseHandler implements ResponseHandler{

    RequestTracer requestTracer;
    Object rpcContext;

    public RequestTracerResponseHandler(Object rpcContext, RequestTracer requestTracer) {
        this.rpcContext = rpcContext;
        this.requestTracer = requestTracer;
    }

    @Override
    public void handle(ResponseMessage responseData) {
        Preconditions.checkNotNull(responseData);

        long responseSize = responseData.getResponse().getEntity().getContentLength();

        RequestTracer.ResponseReceiveTraceInfo responseReceiveTraceInfo = new RequestTracer.ResponseReceiveTraceInfo(responseData.getStatusCode(), responseSize, rpcContext);
        requestTracer.responseReceive(responseReceiveTraceInfo);
    }
}
