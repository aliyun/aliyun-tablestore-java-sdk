package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.PartialResultFailedException;
import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.OTSResult;
import com.aliyun.openservices.ots.model.OTSResultFactory;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.BatchGetRowResponse;

import java.util.List;
import java.util.Map;

class BatchGetRowAsyncResponseConsumer extends
        OTSAsyncResponseConsumer<BatchGetRowResult> {

    private BatchGetRowExecutionContext executionContext;

    public BatchGetRowAsyncResponseConsumer(ResultParser resultParser, BatchGetRowExecutionContext executionContext) {
        super(resultParser, executionContext.getTraceLogger());
        this.executionContext = executionContext;
    }

    /**
     * 1.不是private方法，方便做ut.
     * 2.假设getTableToRowsStatus()返回的结果中index有序
     */
    BatchGetRowResult mergeResult(BatchGetRowResult lastResult, BatchGetRowResult result) {

        OTSResult meta = new OTSResult(result.getRequestID());
        meta.setTraceId(this.traceLogger.getTraceId());
        BatchGetRowResult mergedResult = new BatchGetRowResult(meta);

        Map<String, List<BatchGetRowResult.RowStatus>> lastRowResultMap = lastResult.getTableToRowsStatus();
        Map<String, List<BatchGetRowResult.RowStatus>> rowResultMap = result.getTableToRowsStatus();

        for (String tableName : lastRowResultMap.keySet()) {
            List<BatchGetRowResult.RowStatus> lastRowResultList = lastRowResultMap.get(tableName);
            List<BatchGetRowResult.RowStatus> rowResultList = rowResultMap.get(tableName);
            int idx = 0;
            for (BatchGetRowResult.RowStatus lastRowResult : lastRowResultList) {
                if (lastRowResult.isSucceed()) {
                    mergedResult.addResult(lastRowResult);
                } else {
                    BatchGetRowResult.RowStatus rowResult = rowResultList.get(idx++);
                    if (rowResult.isSucceed()) {
                        mergedResult.addResult(new BatchGetRowResult.RowStatus(tableName, rowResult.getRow(),
                                rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                    } else {
                        mergedResult.addResult(new BatchGetRowResult.RowStatus(tableName, rowResult.getError(),
                                lastRowResult.getIndex()));
                    }
                }
            }
        }
        return mergedResult;
    }
    
    @Override
    protected BatchGetRowResult parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        BatchGetRowResponse batchGetRowResponse = (BatchGetRowResponse) responseContent
                .getMessage();
        BatchGetRowResult result = OTSResultFactory.createBatchGetRowResult(
                responseContent, batchGetRowResponse);

        BatchGetRowResult lastResult = executionContext.getLastResult();
        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }
        List<BatchGetRowResult.RowStatus> failedRows = result.getFailedRows();
        if (failedRows.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestID(), result);
            /**
             * 部分失败时记录log信息
             */
            for (BatchGetRowResult.RowStatus rowResult : failedRows) {
                OTSException ex = new OTSException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestID(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(executionContext, ex, result.getRequestID());
            }
            throw partialEx;
        }
    }

}