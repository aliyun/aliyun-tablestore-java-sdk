/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.comm;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.internal.OTSAsyncResponseConsumer;
import com.aliyun.openservices.ots.internal.OTSTraceLogger;
import com.aliyun.openservices.ots.utils.HttpUtil;
import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.utils.ServiceConstants;
import org.apache.http.concurrent.FutureCallback;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

public abstract class ServiceClient {

    protected static ResourceManager rm = ResourceManager
            .getInstance(ServiceConstants.RESOURCE_NAME_COMMON);

    private ClientConfiguration config;

    protected ServiceClient(ClientConfiguration config) {
        this.config = config;
    }

    public ClientConfiguration getClientConfiguration() {
        return this.config;
    }

    public <Res> void asyncSendRequest(RequestMessage request,
            ExecutionContext context, OTSAsyncResponseConsumer<Res> consumer,
            FutureCallback<Res> callback, OTSTraceLogger traceLogger) {
        assertParameterNotNull(request, "RequestMessage");
        assertParameterNotNull(context, "ExecutionContext");

        context.getSigner().sign(request);
        handleRequest(request, context.getResquestHandlers());
        consumer.setContext(context);
        asyncSendRequestCore(request, context, consumer, callback, traceLogger);
    }

    protected abstract <Res> void asyncSendRequestCore(RequestMessage request,
            ExecutionContext context, OTSAsyncResponseConsumer<Res> consumer,
            FutureCallback<Res> callback, OTSTraceLogger traceLogger);

    protected void handleRequest(RequestMessage message,
            List<RequestHandler> resquestHandlers) throws ClientException {
        for (RequestHandler h : resquestHandlers) {
            h.handle(message);
        }
    }

    public abstract void shutdown();
}
