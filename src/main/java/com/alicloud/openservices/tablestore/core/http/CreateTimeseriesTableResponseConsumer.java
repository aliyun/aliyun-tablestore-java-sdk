package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesTableResponse;

public class CreateTimeseriesTableResponseConsumer extends ResponseConsumer<CreateTimeseriesTableResponse> {

    public CreateTimeseriesTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, CreateTimeseriesTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateTimeseriesTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.CreateTimeseriesTableResponse response =
            (Timeseries.CreateTimeseriesTableResponse) responseContent.getMessage();
        CreateTimeseriesTableResponse result = TimeseriesResponseFactory.createCreateTimeseriesTableResponse(
            responseContent, response);
        return result;
    }
}
