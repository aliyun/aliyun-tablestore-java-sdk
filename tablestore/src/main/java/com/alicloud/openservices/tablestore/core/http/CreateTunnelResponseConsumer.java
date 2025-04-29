package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class CreateTunnelResponseConsumer
    extends ResponseConsumer<CreateTunnelResponse> {
    public CreateTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, CreateTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.CreateTunnelResponse tunnelResponse =
            (TunnelServiceApi.CreateTunnelResponse) responseContent.getMessage();
        CreateTunnelResponse response = ResponseFactory.createCreateTunnelResponse(responseContent, tunnelResponse);
        return response;
    }
}
