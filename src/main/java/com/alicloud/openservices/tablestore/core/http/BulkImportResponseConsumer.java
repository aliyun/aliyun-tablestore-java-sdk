package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.BulkImportResponse;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

import java.util.List;
import java.util.Map;

public class BulkImportResponseConsumer extends ResponseConsumer<BulkImportResponse> {

    public BulkImportResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, BulkImportResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    /**
     * 1.不是private方法，方便做ut.
     * 2.假设getRowStatus返回的结果中index有序.
     */
    BulkImportResponse mergeResult(BulkImportResponse lastResult, BulkImportResponse result) {

        Response meta = new Response(result.getRequestId());
        meta.setTraceId(this.traceLogger.getTraceId());
        BulkImportResponse mergedResult = new BulkImportResponse(meta);

        List<BulkImportResponse.RowResult> lastRowResultList = lastResult.getRowResults();
        List<BulkImportResponse.RowResult> rowResultList = result.getRowResults();
        int idx = 0;
        for (BulkImportResponse.RowResult lastRowResult : lastRowResultList) {
            if (lastRowResult.isSucceed()) {
                mergedResult.addRowResult(lastRowResult);
            } else {
                BulkImportResponse.RowResult rowResult = rowResultList.get(idx);
                idx++;
                if (rowResult.isSucceed()) {
                    mergedResult.addRowResult(new BulkImportResponse.RowResult(
                            rowResult.getConsumedCapacity(), lastRowResult.getIndex()));
                } else {
                    mergedResult.addRowResult(new BulkImportResponse.RowResult(
                            rowResult.getError(), lastRowResult.getIndex()));
                }
            }
        }
        return mergedResult;
    }

    @Override
    protected BulkImportResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.BulkImportResponse bulkImportResponse =
                (OtsInternalApi.BulkImportResponse) responseContent.getMessage();
        BulkImportResponse result = ResponseFactory.createBulkImportResponse(
                responseContent, bulkImportResponse);

        if (lastResult != null) {
            result = mergeResult(lastResult, result);
        }

        List<BulkImportResponse.RowResult> failedRows = result.getFailedRows();
        if (failedRows.isEmpty()) {
            return result;
        } else {
            PartialResultFailedException partialEx = new PartialResultFailedException(null, result.getRequestId(), result);
            /**
             * 部分失败时记录log信息
             */
            for (BulkImportResponse.RowResult rowResult : failedRows) {
                TableStoreException ex = new TableStoreException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(),
                        result.getRequestId(), 0);
                partialEx.addError(ex);
                LogUtil.logOnFailed(traceLogger, retry, ex, result.getRequestId());
            }

            throw partialEx;
        }
    }

}