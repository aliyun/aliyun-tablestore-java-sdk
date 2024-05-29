package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesLastpointIndexResponse;

public class DeleteTimeseriesLastpointIndexResponseConsumer extends ResponseConsumer<DeleteTimeseriesLastpointIndexResponse> {

    public DeleteTimeseriesLastpointIndexResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DeleteTimeseriesLastpointIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTimeseriesLastpointIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DeleteTimeseriesLastpointIndexResponse response =
                (Timeseries.DeleteTimeseriesLastpointIndexResponse) responseContent.getMessage();
        DeleteTimeseriesLastpointIndexResponse result = TimeseriesResponseFactory.createDeleteTimeseriesLastpointIndexResponse(
                responseContent, response);
        return result;
    }
}
