package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.ListTimeseriesTableResponse;

public class ListTimeseriesTableResponseConsumer extends ResponseConsumer<ListTimeseriesTableResponse> {

    public ListTimeseriesTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, ListTimeseriesTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListTimeseriesTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.ListTimeseriesTableResponse response =
            (Timeseries.ListTimeseriesTableResponse) responseContent.getMessage();
        ListTimeseriesTableResponse result = TimeseriesResponseFactory.createListTimeseriesTableResponse(
            responseContent, response);
        return result;
    }
}
