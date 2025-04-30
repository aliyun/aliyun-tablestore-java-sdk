package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;

public class ConnectTunnelResponseConsumer
    extends ResponseConsumer<ConnectTunnelResponse> {
    public ConnectTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, ConnectTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ConnectTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.ConnectResponse tunnelResponse =
            (TunnelServiceApi.ConnectResponse)responseContent.getMessage();
        ConnectTunnelResponse response = ResponseFactory.createConnectTunnelResponse(responseContent, tunnelResponse);
        return response;
    }
}
