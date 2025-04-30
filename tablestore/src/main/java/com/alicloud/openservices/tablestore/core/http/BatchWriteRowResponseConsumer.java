package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;

import java.util.List;
import java.util.Map;

public class BatchWriteRowResponseConsumer extends ResponseConsumer<BatchWriteRowResponse> {

    public BatchWriteRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, BatchWriteRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    /**
     * 1. Not a private method, for convenience to perform unit testing (ut).
     * 2. Assume that the results returned by getRowStatus are index-ordered.
     */
    BatchWriteRowResponse mergeResult(BatchWriteRowResponse lastResult, BatchWriteRowResponse result) {

    	Response meta = new Response(result.getRequestId());
        meta.setTraceId(this.traceLogger.getTraceId());
        BatchWriteRowResponse mergedResult = new BatchWriteRowResponse(meta);

        Map<String, List<BatchWriteRowResponse.RowResult>> lastRowResultMap = lastResult.getRowStatus();
        Map<String, List<BatchWriteRowResponse.RowResult>> rowResultMap = result.getRowStatus();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchWriteRowResponse.RowResult> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchWriteRowResponse.RowResult> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchWriteRowResponse.RowResult lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addRowResult(lastRowResult);
                } else {
                    BatchWriteRowResponse.RowResult rowResult = rowResultList.get(idx);
                    idx++;
                    if (rowResult.isSucceed()) {
                        mergedResult.addRowResult(new BatchWriteRowResponse.RowResult(tableName,
                                rowResult.getRow(), rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addRowResult(new BatchWriteRowResponse.RowResult(tableName,
                                rowResult.getRow(), rowResult.getError(), lastRowResult.getIndex()));
                    }
                }
            }
        }

        return mergedResult;
    }

    @Override
    protected BatchWriteRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.BatchWriteRowResponse BatchWriteRowResponse =
            (OtsInternalApi.BatchWriteRowResponse) responseContent.getMessage();
        BatchWriteRowResponse result = ResponseFactory.createBatchWriteRowResponse(
            responseContent, BatchWriteRowResponse);
        
        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }
        List<BatchWriteRowResponse.RowResult> failedRows = result.getFailedRows();
        if (failedRows.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestId(), result);
            /**
             * Record log information when partially failed
             */
            for (BatchWriteRowResponse.RowResult rowResult : failedRows) {
                TableStoreException ex = new TableStoreException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestId(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(traceLogger, retry, ex, result.getRequestId());
            }

            throw partialEx;
        }
    }
        
}
