package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import com.google.common.cache.Cache;

public class PutTimeseriesDataResponseConsumer extends ResponseConsumer<PutTimeseriesDataResponse> {

    private PutTimeseriesDataRequest request;
    private Cache<String, Long> timeseriesMetaCache;

    public PutTimeseriesDataResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry,
                                             PutTimeseriesDataResponse lastResult, PutTimeseriesDataRequest request,
                                             Cache<String, Long> timeseriesMetaCache) {
        super(resultParser, traceLogger, retry, lastResult);
        this.request = request;
        this.timeseriesMetaCache = timeseriesMetaCache;
    }

    @Override
    protected PutTimeseriesDataResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Timeseries.PutTimeseriesDataResponse response =
            (Timeseries.PutTimeseriesDataResponse) responseContent.getMessage();
        PutTimeseriesDataResponse result = TimeseriesResponseFactory.createPutTimeseriesDataResponse(
            responseContent, response, request, timeseriesMetaCache);
        return result;
    }
}
