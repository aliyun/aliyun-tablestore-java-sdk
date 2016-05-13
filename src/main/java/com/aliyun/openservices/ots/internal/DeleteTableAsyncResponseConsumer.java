package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.DeleteTableResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteTableResponse;

class DeleteTableAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<DeleteTableResult> {

    public DeleteTableAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected DeleteTableResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        DeleteTableResponse deleteTableResponse = (DeleteTableResponse) responseContent
                .getMessage();
        DeleteTableResult result = OTSResultFactory.createDeleteTableResult(
                responseContent, deleteTableResponse);
        return result;
    }
}