package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.QueryTimeseriesMetaResponse;

public class QueryTimeseriesMetaResponseConsumer extends ResponseConsumer<QueryTimeseriesMetaResponse> {

    public QueryTimeseriesMetaResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, QueryTimeseriesMetaResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected QueryTimeseriesMetaResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.QueryTimeseriesMetaResponse response =
            (Timeseries.QueryTimeseriesMetaResponse) responseContent.getMessage();
        QueryTimeseriesMetaResponse result = TimeseriesResponseFactory.createQueryTimeseriesMetaResponse(
            responseContent, response);
        return result;
    }
}
