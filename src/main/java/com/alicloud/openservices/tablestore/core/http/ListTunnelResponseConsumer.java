package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ListTunnelResponseConsumer
    extends ResponseConsumer<ListTunnelResponse> {
    public ListTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, ListTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.ListTunnelResponse tunnelResponse =
            (TunnelServiceApi.ListTunnelResponse)responseContent.getMessage();
        ListTunnelResponse response = ResponseFactory.createListTunnelResponse(responseContent, tunnelResponse);
        return response;
    }
}
