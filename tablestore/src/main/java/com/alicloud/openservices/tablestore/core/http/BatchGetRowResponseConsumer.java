package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;

import java.util.List;
import java.util.Map;

public class BatchGetRowResponseConsumer extends ResponseConsumer<BatchGetRowResponse> {

    public BatchGetRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, BatchGetRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    /**
     * 1. Not a private method, convenient for unit testing.
     * 2. Assumes that the results returned by getTableToRowsStatus() are sorted by index.
     */
    BatchGetRowResponse mergeResult(BatchGetRowResponse lastResult, BatchGetRowResponse result) {

        Response meta = new Response(result.getRequestId());
        meta.setTraceId(this.traceLogger.getTraceId());
        BatchGetRowResponse mergedResult = new BatchGetRowResponse(meta);

        Map<String, List<BatchGetRowResponse.RowResult>> lastRowResultMap = lastResult.getTableToRowsResult();
        Map<String, List<BatchGetRowResponse.RowResult>> rowResultMap = result.getTableToRowsResult();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchGetRowResponse.RowResult> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchGetRowResponse.RowResult> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchGetRowResponse.RowResult lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addResult(lastRowResult);
                } else {
                	BatchGetRowResponse.RowResult rowResult = rowResultList.get(idx++);
                    if (rowResult.isSucceed()) {
                        mergedResult.addResult(new BatchGetRowResponse.RowResult(tableName, rowResult.getRow(),
                                rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addResult(new BatchGetRowResponse.RowResult(tableName, rowResult.getError(),
                                lastRowResult.getIndex()));
                    }
                }
            }
        }
        return mergedResult;
    }

    @Override
    protected BatchGetRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.BatchGetRowResponse BatchGetRowResponse =
            (OtsInternalApi.BatchGetRowResponse) responseContent.getMessage();
        BatchGetRowResponse result = ResponseFactory.createBatchGetRowResponse(
            responseContent, BatchGetRowResponse);

        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }
        List<BatchGetRowResponse.RowResult> failedRows = result.getFailedRows();
        if (failedRows.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestId(), result);
            /**
             * Record log information when partially failed
             */
            for (BatchGetRowResponse.RowResult rowResult : failedRows) {
                TableStoreException ex = new TableStoreException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestId(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(traceLogger, retry, ex, result.getRequestId());
            }
            throw partialEx;
        }
    }
        
}
