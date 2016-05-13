package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.model.UpdateTableResult;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateTableResponse;

class UpdateTableAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<UpdateTableResult> {

    public UpdateTableAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected UpdateTableResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        UpdateTableResponse updateTableResponse = (UpdateTableResponse) responseContent
                .getMessage();
        UpdateTableResult result = OTSResultFactory.createUpdateTableResult(
                responseContent, updateTableResponse);
        return result;
    }
}