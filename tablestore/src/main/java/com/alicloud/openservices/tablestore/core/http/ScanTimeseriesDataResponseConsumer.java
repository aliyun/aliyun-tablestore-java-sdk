package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;

public class ScanTimeseriesDataResponseConsumer extends ResponseConsumer<ScanTimeseriesDataResponse> {

    public ScanTimeseriesDataResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, ScanTimeseriesDataResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ScanTimeseriesDataResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.ScanTimeseriesDataResponse response =
                (Timeseries.ScanTimeseriesDataResponse) responseContent.getMessage();
        ScanTimeseriesDataResponse result = TimeseriesResponseFactory.createScanTimeseriesDataResponse(
                responseContent, response);
        return result;
    }
}
