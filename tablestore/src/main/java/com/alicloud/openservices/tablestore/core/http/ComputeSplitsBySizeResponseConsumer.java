package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ComputeSplitsBySizeResponseConsumer extends ResponseConsumer<ComputeSplitsBySizeResponse> {

    public ComputeSplitsBySizeResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry,
            ComputeSplitsBySizeResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ComputeSplitsBySizeResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.ComputeSplitPointsBySizeResponse ComputeSplitPointsBySizeResponse =
                (OtsInternalApi.ComputeSplitPointsBySizeResponse) responseContent.getMessage();
        ComputeSplitsBySizeResponse result = ResponseFactory.createComputeSplitsBySizeResponse(
                responseContent, ComputeSplitPointsBySizeResponse);
        return result;
    }

}
