/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.GetRowResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRowResponse;

class GetRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<GetRowResult> {

    public GetRowAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected GetRowResult parseResult()
            throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GetRowResponse getRowResponse = (GetRowResponse) responseContent
                .getMessage();
        GetRowResult result = OTSResultFactory.createGetRowResult(
                responseContent, getRowResponse);
        return result;
    }
}