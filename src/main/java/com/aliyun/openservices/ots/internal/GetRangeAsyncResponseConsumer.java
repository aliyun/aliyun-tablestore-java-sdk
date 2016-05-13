package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRangeResponse;

class GetRangeAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<GetRangeResult> {

    public GetRangeAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected GetRangeResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GetRangeResponse getRangeResponse = (GetRangeResponse) responseContent
                .getMessage();
        GetRangeResult result = OTSResultFactory.createGetRangeResult(
                responseContent, getRangeResponse);
        return result;
    }
}