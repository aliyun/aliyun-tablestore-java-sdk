package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.ListTableResponse;

class ListTableAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<ListTableResult> {

    public ListTableAsyncResponseConsumer(ResultParser resultParser, OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected ListTableResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        ListTableResponse listTableResponse = (ListTableResponse) responseContent
                .getMessage();
        ListTableResult result = OTSResultFactory.createListTableResult(
                responseContent, listTableResponse);
        return result;
    }
}