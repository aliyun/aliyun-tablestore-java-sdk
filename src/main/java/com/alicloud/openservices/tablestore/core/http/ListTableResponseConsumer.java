package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class ListTableResponseConsumer
  extends ResponseConsumer<ListTableResponse> {

    public ListTableResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, ListTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.ListTableResponse internalResponse =
            (OtsInternalApi.ListTableResponse) responseContent.getMessage();
        ListTableResponse response = ResponseFactory.createListTableResponse(
            responseContent, internalResponse);
        return response;
    }
}
