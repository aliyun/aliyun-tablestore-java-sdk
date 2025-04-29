package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesLastpointIndexResponse;

public class CreateTimeseriesLastpointIndexResponseConsumer extends ResponseConsumer<CreateTimeseriesLastpointIndexResponse> {

    public CreateTimeseriesLastpointIndexResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, CreateTimeseriesLastpointIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateTimeseriesLastpointIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.CreateTimeseriesLastpointIndexResponse response =
                (Timeseries.CreateTimeseriesLastpointIndexResponse) responseContent.getMessage();
        CreateTimeseriesLastpointIndexResponse result = TimeseriesResponseFactory.createCreateTimeseriesLastpointIndexResponse(
                responseContent, response);
        return result;
    }
}
