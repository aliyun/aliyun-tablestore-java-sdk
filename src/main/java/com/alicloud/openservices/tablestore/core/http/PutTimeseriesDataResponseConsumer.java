package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesResponseFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import com.google.common.cache.Cache;

import java.util.ArrayList;
import java.util.List;

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

        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }
        List<PutTimeseriesDataResponse.FailedRowResult> failedRows = result.getFailedRows();
        if (failedRows.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestId(), result);
            /**
             * 部分失败时记录log信息
             */
            for (PutTimeseriesDataResponse.FailedRowResult rowResult : failedRows) {
                TableStoreException ex = new TableStoreException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestId(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(traceLogger, retry, ex, result.getRequestId());
            }

            throw partialEx;
        }
    }

    private PutTimeseriesDataResponse mergeResult(PutTimeseriesDataResponse lastResult, PutTimeseriesDataResponse result) {
        Response meta = new Response(result.getRequestId());
        meta.setTraceId(this.traceLogger.getTraceId());
        PutTimeseriesDataResponse mergedResult = new PutTimeseriesDataResponse(meta);

        List<PutTimeseriesDataResponse.FailedRowResult> lastRowResultList = lastResult.getFailedRows();
        List<PutTimeseriesDataResponse.FailedRowResult> rowResultList = result.getFailedRows();
        List<PutTimeseriesDataResponse.FailedRowResult> mergedResultList = new ArrayList<PutTimeseriesDataResponse.FailedRowResult>();

        for (PutTimeseriesDataResponse.FailedRowResult r : rowResultList) {
            int originIndex = lastRowResultList.get(r.getIndex()).getIndex();
            PutTimeseriesDataResponse.FailedRowResult mergedRowResult =
                    new PutTimeseriesDataResponse.FailedRowResult(originIndex, r.getError());

            mergedResultList.add(mergedRowResult);
        }
        mergedResult.setFailedRows(mergedResultList);
        return mergedResult;
    }
}
