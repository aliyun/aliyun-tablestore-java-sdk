/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.DescribeTableResponse;

class DescribeTableAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<DescribeTableResult> {

    public DescribeTableAsyncResponseConsumer(ResultParser resultParser,
            OTSTraceLogger traceLogger) {
        super(resultParser, traceLogger);
    }

    @Override
    protected DescribeTableResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        DescribeTableResponse describeTableResponse = (DescribeTableResponse) responseContent
                .getMessage();
        DescribeTableResult result = OTSResultFactory
                .createDescribeTableResult(responseContent,
                        describeTableResponse);
        return result;
    }
}