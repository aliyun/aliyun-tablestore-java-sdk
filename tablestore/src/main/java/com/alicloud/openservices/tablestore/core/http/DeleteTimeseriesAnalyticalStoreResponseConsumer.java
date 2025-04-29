package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesAnalyticalStoreResponse;

public class DeleteTimeseriesAnalyticalStoreResponseConsumer extends ResponseConsumer<DeleteTimeseriesAnalyticalStoreResponse> {

    public DeleteTimeseriesAnalyticalStoreResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DeleteTimeseriesAnalyticalStoreResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTimeseriesAnalyticalStoreResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DeleteTimeseriesAnalyticalStoreResponse response =
                (Timeseries.DeleteTimeseriesAnalyticalStoreResponse) responseContent.getMessage();
        DeleteTimeseriesAnalyticalStoreResponse result = TimeseriesResponseFactory.createDeleteTimeseriesAnalyticalStoreResponse(
                responseContent, response);
        return result;
    }
}
