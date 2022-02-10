package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.GetTimeseriesDataResponse;

public class GetTimeseriesDataResponseConsumer extends ResponseConsumer<GetTimeseriesDataResponse> {

    public GetTimeseriesDataResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, GetTimeseriesDataResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetTimeseriesDataResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.GetTimeseriesDataResponse response =
            (Timeseries.GetTimeseriesDataResponse) responseContent.getMessage();
        GetTimeseriesDataResponse result = TimeseriesResponseFactory.createGetTimeseriesDataResponse(
            responseContent, response);
        return result;
    }
}
