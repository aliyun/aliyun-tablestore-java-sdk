package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class GetStreamRecordResponseConsumer
        extends ResponseConsumer<GetStreamRecordResponse> {

    public GetStreamRecordResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, GetStreamRecordResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetStreamRecordResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.GetStreamRecordResponse internalResponse =
                (OtsInternalApi.GetStreamRecordResponse) responseContent.getMessage();
        GetStreamRecordResponse response = ResponseFactory.createGetStreamRecordResponse(
                responseContent, internalResponse);
        return response;
    }
}
