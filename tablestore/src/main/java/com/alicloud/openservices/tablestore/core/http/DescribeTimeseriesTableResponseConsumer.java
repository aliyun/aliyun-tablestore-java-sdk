package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesTableResponse;

public class DescribeTimeseriesTableResponseConsumer extends ResponseConsumer<DescribeTimeseriesTableResponse> {

    public DescribeTimeseriesTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DescribeTimeseriesTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeTimeseriesTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.DescribeTimeseriesTableResponse response =
            (Timeseries.DescribeTimeseriesTableResponse) responseContent.getMessage();
        DescribeTimeseriesTableResponse result = TimeseriesResponseFactory.createDescribeTimeseriesTableResponse(
            responseContent, response);
        return result;
    }
}
