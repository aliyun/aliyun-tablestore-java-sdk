package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;

public class ReadRecordsResponseConsumer
    extends ResponseConsumer<ReadRecordsResponse> {
    public ReadRecordsResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, ReadRecordsResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ReadRecordsResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.ReadRecordsResponse tunnelResponse =
            (TunnelServiceApi.ReadRecordsResponse)responseContent.getMessage();
        return ResponseFactory.createReadRecordsResponse(responseContent, tunnelResponse);
    }
}
