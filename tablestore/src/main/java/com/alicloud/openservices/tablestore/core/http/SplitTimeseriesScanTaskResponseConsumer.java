package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.SplitTimeseriesScanTaskResponse;

public class SplitTimeseriesScanTaskResponseConsumer extends ResponseConsumer<SplitTimeseriesScanTaskResponse> {

    public SplitTimeseriesScanTaskResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, SplitTimeseriesScanTaskResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected SplitTimeseriesScanTaskResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.SplitTimeseriesScanTaskResponse response =
                (Timeseries.SplitTimeseriesScanTaskResponse) responseContent.getMessage();
        SplitTimeseriesScanTaskResponse result = TimeseriesResponseFactory.createSplitTimeseriesScanTaskResponse(
                responseContent, response);
        return result;
    }
}
