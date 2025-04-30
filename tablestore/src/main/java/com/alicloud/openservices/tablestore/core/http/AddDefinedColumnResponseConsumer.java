package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.AddDefinedColumnResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class AddDefinedColumnResponseConsumer
    extends ResponseConsumer<AddDefinedColumnResponse> {

    public AddDefinedColumnResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, AddDefinedColumnResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }
    @Override
    protected AddDefinedColumnResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.AddDefinedColumnResponse internalResponse =
            (OtsInternalApi.AddDefinedColumnResponse) responseContent.getMessage();
            AddDefinedColumnResponse response = ResponseFactory.createAddDefinedColumnResponse(
                responseContent, internalResponse);
            return response;
        }
}
