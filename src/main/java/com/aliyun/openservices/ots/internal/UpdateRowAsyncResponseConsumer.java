package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.model.UpdateRowResult;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateRowResponse;

class UpdateRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<UpdateRowResult> {

    public UpdateRowAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected UpdateRowResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        UpdateRowResponse updateRowResponse = (UpdateRowResponse) responseContent
                .getMessage();
        UpdateRowResult result = OTSResultFactory.createUpdateRowResult(
                responseContent, updateRowResponse);
        return result;
    }
}