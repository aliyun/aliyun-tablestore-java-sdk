package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesAnalyticalStoreResponse;

public class UpdateTimeseriesAnalyticalStoreResponseConsumer extends ResponseConsumer<UpdateTimeseriesAnalyticalStoreResponse> {

    public UpdateTimeseriesAnalyticalStoreResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UpdateTimeseriesAnalyticalStoreResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateTimeseriesAnalyticalStoreResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.UpdateTimeseriesAnalyticalStoreResponse response =
                (Timeseries.UpdateTimeseriesAnalyticalStoreResponse) responseContent.getMessage();
        UpdateTimeseriesAnalyticalStoreResponse result = TimeseriesResponseFactory.createUpdateTimeseriesAnalyticalStoreResponse(
                responseContent, response);
        return result;
    }
}
