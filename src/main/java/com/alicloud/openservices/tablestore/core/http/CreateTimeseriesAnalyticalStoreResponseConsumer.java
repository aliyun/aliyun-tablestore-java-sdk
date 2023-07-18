package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesAnalyticalStoreResponse;

public class CreateTimeseriesAnalyticalStoreResponseConsumer extends ResponseConsumer<CreateTimeseriesAnalyticalStoreResponse> {

    public CreateTimeseriesAnalyticalStoreResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, CreateTimeseriesAnalyticalStoreResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateTimeseriesAnalyticalStoreResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.CreateTimeseriesAnalyticalStoreResponse response =
                (Timeseries.CreateTimeseriesAnalyticalStoreResponse) responseContent.getMessage();
        CreateTimeseriesAnalyticalStoreResponse result = TimeseriesResponseFactory.createCreateTimeseriesAnalyticalStoreResponse(
                responseContent, response);
        return result;
    }
}
