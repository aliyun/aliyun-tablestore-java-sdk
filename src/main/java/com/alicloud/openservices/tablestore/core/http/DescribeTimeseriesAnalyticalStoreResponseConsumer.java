package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesAnalyticalStoreResponse;

public class DescribeTimeseriesAnalyticalStoreResponseConsumer extends ResponseConsumer<DescribeTimeseriesAnalyticalStoreResponse> {

    public DescribeTimeseriesAnalyticalStoreResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DescribeTimeseriesAnalyticalStoreResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeTimeseriesAnalyticalStoreResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DescribeTimeseriesAnalyticalStoreResponse response =
                (Timeseries.DescribeTimeseriesAnalyticalStoreResponse) responseContent.getMessage();
        DescribeTimeseriesAnalyticalStoreResponse result = TimeseriesResponseFactory.createDescribeTimeseriesAnalyticalStoreResponse(
                responseContent, response);
        return result;
    }
}
