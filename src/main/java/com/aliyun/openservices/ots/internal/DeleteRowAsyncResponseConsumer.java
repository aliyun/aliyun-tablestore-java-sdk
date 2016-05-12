/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.DeleteRowResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteRowResponse;

class DeleteRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<DeleteRowResult> {

    public DeleteRowAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected DeleteRowResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        DeleteRowResponse deleteRowResponse = (DeleteRowResponse) responseContent
                .getMessage();
        DeleteRowResult result = OTSResultFactory.createDeleteRowResult(
                responseContent, deleteRowResponse);
        return result;
    }
}