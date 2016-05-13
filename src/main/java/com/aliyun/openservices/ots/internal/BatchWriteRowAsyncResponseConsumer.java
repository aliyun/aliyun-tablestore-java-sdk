package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.PartialResultFailedException;
import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.OTSResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2;

import java.util.List;
import java.util.Map;

class BatchWriteRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<BatchWriteRowResult> {

    private BatchWriteRowExecutionContext executionContext;

    public BatchWriteRowAsyncResponseConsumer(ResultParser resultParser, BatchWriteRowExecutionContext executionContext) {
        super(resultParser, executionContext.getTraceLogger());
        this.executionContext = executionContext;
    }

    /**
     * 1.不是private方法，方便做ut.
     * 2.假设getRowStatus返回的结果中index有序.
     */
    BatchWriteRowResult mergeResult(BatchWriteRowResult lastResult, BatchWriteRowResult result) {
        OTSResult meta = new OTSResult(result.getRequestID());
        meta.setTraceId(this.traceLogger.getTraceId());
        BatchWriteRowResult mergedResult = new BatchWriteRowResult(meta);

        Map<String, List<BatchWriteRowResult.RowStatus>> lastRowResultMap = lastResult.getPutRowStatus();
        Map<String, List<BatchWriteRowResult.RowStatus>> rowResultMap = result.getPutRowStatus();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchWriteRowResult.RowStatus> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchWriteRowResult.RowStatus> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchWriteRowResult.RowStatus lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addPutRowResult(lastRowResult);
                } else {
                    BatchWriteRowResult.RowStatus rowResult = rowResultList.get(idx);
                    idx++;
                    if (rowResult.isSucceed()) {
                        mergedResult.addPutRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addPutRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getError(), lastRowResult.getIndex()));
                    }
                }
            }
        }

        lastRowResultMap = lastResult.getUpdateRowStatus();
        rowResultMap = result.getUpdateRowStatus();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchWriteRowResult.RowStatus> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchWriteRowResult.RowStatus> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchWriteRowResult.RowStatus lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addUpdateRowResult(lastRowResult);
                } else {
                    BatchWriteRowResult.RowStatus rowResult = rowResultList.get(idx);
                    idx++;
                    if (rowResult.isSucceed()) {
                        mergedResult.addUpdateRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addUpdateRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getError(), lastRowResult.getIndex()));
                    }
                }
            }
        }

        lastRowResultMap = lastResult.getDeleteRowStatus();
        rowResultMap = result.getDeleteRowStatus();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchWriteRowResult.RowStatus> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchWriteRowResult.RowStatus> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchWriteRowResult.RowStatus lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addDeleteRowResult(lastRowResult);
                } else {
                    BatchWriteRowResult.RowStatus rowResult = rowResultList.get(idx);
                    idx++;
                    if (rowResult.isSucceed()) {
                        mergedResult.addDeleteRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addDeleteRowResult(new BatchWriteRowResult.RowStatus(tableName,
                                rowResult.getError(), lastRowResult.getIndex()));
                    }
                }
            }
        }

        return mergedResult;
    }

    @Override
    protected BatchWriteRowResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsProtocol2.BatchWriteRowResponse batchWriteRowResponse = (OtsProtocol2.BatchWriteRowResponse) responseContent
                .getMessage();
        BatchWriteRowResult result = OTSResultFactory
                .createBatchWriteRowResult(responseContent, batchWriteRowResponse);

        BatchWriteRowResult lastResult = executionContext.getLastResult();
        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }
        List<BatchWriteRowResult.RowStatus> failedRowsOfPut = result.getFailedRowsOfPut();
        List<BatchWriteRowResult.RowStatus> failedRowsOfUpdate = result.getFailedRowsOfUpdate();
        List<BatchWriteRowResult.RowStatus> failedRowsOfDelete = result.getFailedRowsOfDelete();
        if (failedRowsOfPut.isEmpty() && failedRowsOfUpdate.isEmpty() && failedRowsOfDelete.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestID(), result);
            /**
             * 部分失败时记录log信息
             */
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsOfPut) {
                OTSException ex = new OTSException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestID(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(executionContext, ex, result.getRequestID());
            }
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsOfUpdate) {
                OTSException ex = new OTSException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestID(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(executionContext, ex, result.getRequestID());
            }
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsOfDelete) {
                OTSException ex = new OTSException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestID(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(executionContext, ex, result.getRequestID());
            }

            throw partialEx;
        }
    }
}