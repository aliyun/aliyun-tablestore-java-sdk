package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesTableResponse;

public class DeleteTimeseriesTableResponseConsumer extends ResponseConsumer<DeleteTimeseriesTableResponse> {

    public DeleteTimeseriesTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DeleteTimeseriesTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTimeseriesTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DeleteTimeseriesTableResponse response =
            (Timeseries.DeleteTimeseriesTableResponse) responseContent.getMessage();
        DeleteTimeseriesTableResponse result = TimeseriesResponseFactory.createDeleteTimeseriesTableResponse(
            responseContent, response);
        return result;
    }
}
