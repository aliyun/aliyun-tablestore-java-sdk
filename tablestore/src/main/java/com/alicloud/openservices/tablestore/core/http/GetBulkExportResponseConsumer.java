package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.BulkExportResponse;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class GetBulkExportResponseConsumer extends ResponseConsumer<BulkExportResponse> {

    public GetBulkExportResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, BulkExportResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected BulkExportResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.BulkExportResponse bulkExportResponse =
                (OtsInternalApi.BulkExportResponse) responseContent.getMessage();
        BulkExportResponse result = ResponseFactory.createBulkExportResponse(
                responseContent, bulkExportResponse);
        return result;
    }

}
