package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesMetaResponse;

public class DeleteTimeseriesMetaResponseConsumer extends ResponseConsumer<DeleteTimeseriesMetaResponse> {

    public DeleteTimeseriesMetaResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DeleteTimeseriesMetaResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTimeseriesMetaResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DeleteTimeseriesMetaResponse response =
                (Timeseries.DeleteTimeseriesMetaResponse) responseContent.getMessage();
        DeleteTimeseriesMetaResponse result = TimeseriesResponseFactory.createDeleteTimeseriesMetaResponse(
                responseContent, response);
        return result;
    }
}
