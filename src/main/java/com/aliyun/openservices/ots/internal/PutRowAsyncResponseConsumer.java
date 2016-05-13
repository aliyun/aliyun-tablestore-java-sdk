package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.model.PutRowResult;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.PutRowResponse;

class PutRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<PutRowResult> {

    public PutRowAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected PutRowResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        PutRowResponse putRowResponse = (PutRowResponse) responseContent
                .getMessage();
        PutRowResult result = OTSResultFactory.createPutRowResult(
                responseContent, putRowResponse);
        return result;
    }
}