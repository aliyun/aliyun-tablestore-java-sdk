package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesTableResponse;

public class UpdateTimeseriesTableResponseConsumer extends ResponseConsumer<UpdateTimeseriesTableResponse> {

    public UpdateTimeseriesTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UpdateTimeseriesTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateTimeseriesTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.UpdateTimeseriesTableResponse response =
            (Timeseries.UpdateTimeseriesTableResponse) responseContent.getMessage();
        UpdateTimeseriesTableResponse result = TimeseriesResponseFactory.createUpdateTimeseriesTableResponse(
            responseContent, response);
        return result;
    }
}
