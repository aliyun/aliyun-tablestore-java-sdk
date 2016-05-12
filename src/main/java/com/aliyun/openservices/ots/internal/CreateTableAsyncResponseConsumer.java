/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.CreateTableResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.CreateTableResponse;

class CreateTableAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<CreateTableResult> {

    public CreateTableAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected CreateTableResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        CreateTableResponse createTableResponse = (CreateTableResponse) responseContent
                .getMessage();
        CreateTableResult result = OTSResultFactory.createCreateTableResult(
                responseContent, createTableResponse);
        return result;
    }
}