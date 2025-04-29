package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesMetaResponse;

public class UpdateTimeseriesMetaResponseConsumer extends ResponseConsumer<UpdateTimeseriesMetaResponse> {

    public UpdateTimeseriesMetaResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UpdateTimeseriesMetaResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateTimeseriesMetaResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.UpdateTimeseriesMetaResponse response =
                (Timeseries.UpdateTimeseriesMetaResponse) responseContent.getMessage();
        UpdateTimeseriesMetaResponse result = TimeseriesResponseFactory.createUpdateTimeseriesMetaResponse(
                responseContent, response);
        return result;
    }
}
