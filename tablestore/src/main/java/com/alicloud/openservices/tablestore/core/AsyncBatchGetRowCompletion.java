package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Error;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class AsyncBatchGetRowCompletion
        extends AsyncCompletion<BatchGetRowRequest, BatchGetRowResponse> {

    static class PartialRowResult {
        Error error;
        PrimaryKey primaryKey;
        List<Column> columns;
        ConsumedCapacity consumedCapacity;
        byte[] nextToken;

        boolean isComplete() {
            return (error != null) || (nextToken == null);
        }
    }

    BatchGetRowRequest nextRequest;
    Map<String, PartialRowResult[]> tableToResults;
    List<String> requestIds;

    public AsyncBatchGetRowCompletion(
            OperationLauncher launcher,
            BatchGetRowRequest request, TraceLogger tracer,
            ExecutorService callbackExecutor,
            RetryStrategy retry, ScheduledExecutorService retryExecutor)
    {
        super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
    }

    @Override
    public void completed(BatchGetRowResponse result) {
        result.setTraceId(tracer.getTraceId());
        LogUtil.logOnCompleted(tracer, retry, result.getRequestId());
        tracer.printLog();

        if (isCompleted(result)) {
            // If it's not the first request, the results need to be merged with the existing ones.
            if (nextRequest != null) {
                handleResult(result);
                onCompleted(request, buildFinalResult());
            } else {
                onCompleted(request, result);
            }
        } else {
            handleResult(result);
            sendNextRequest();
        }
    }

    // Among all successful rows, if there is a nextToken, a request needs to be initiated continuously.
    boolean isCompleted(BatchGetRowResponse result) {
        for (BatchGetRowResponse.RowResult rowResult : result.getSucceedRows()) {
            if (rowResult.hasNextToken()) {
                return false;
            }
        }
        return true;
    }

    void handleConsumedCapacity(ConsumedCapacity base, ConsumedCapacity delta) {
        base.getCapacityUnit().setReadCapacityUnit(base.getCapacityUnit().getReadCapacityUnit() +
                                                    delta.getCapacityUnit().getReadCapacityUnit());
        base.getCapacityUnit().setWriteCapacityUnit(base.getCapacityUnit().getWriteCapacityUnit() +
                                                    delta.getCapacityUnit().getWriteCapacityUnit());
    }

    void handleResult(BatchGetRowResponse result) {
        // Process the first Result
        if (tableToResults == null) {
            tableToResults = new ConcurrentHashMap<String, PartialRowResult[]>();
            for (Map.Entry<String, List<BatchGetRowResponse.RowResult>> entry : result.getTableToRowsResult().entrySet()) {
                PartialRowResult[] partialRowResults = new PartialRowResult[entry.getValue().size()];
                tableToResults.put(entry.getKey(), partialRowResults);
                int idx = 0;
                for (BatchGetRowResponse.RowResult rowResult : entry.getValue()) {
                    partialRowResults[idx] = new PartialRowResult();
                    if (rowResult.isSucceed()) {
                        partialRowResults[idx].consumedCapacity = rowResult.getConsumedCapacity();
                        if (rowResult.hasNextToken()) {
                            partialRowResults[idx].nextToken = rowResult.getNextToken();
                        }
                        if (rowResult.getRow() != null) {
                            partialRowResults[idx].primaryKey = rowResult.getRow().getPrimaryKey();
                            partialRowResults[idx].columns = new ArrayList<Column>(Arrays.asList(rowResult.getRow().getColumns()));
                        }
                    } else {
                        partialRowResults[idx].error = rowResult.getError();
                    }
                    idx++;
                }
            }
        } else {
            for (Map.Entry<String, List<BatchGetRowResponse.RowResult>> entry : result.getTableToRowsResult().entrySet()) {
                PartialRowResult[] partialRowResults = tableToResults.get(entry.getKey());
                int idx = 0;
                for (BatchGetRowResponse.RowResult rowResult : entry.getValue()) {
                    while (partialRowResults[idx].isComplete()) {
                        idx++;
                    }
                    if (rowResult.isSucceed()) {
                        handleConsumedCapacity(partialRowResults[idx].consumedCapacity, rowResult.getConsumedCapacity());
                        if (rowResult.hasNextToken()) {
                            partialRowResults[idx].nextToken = rowResult.getNextToken();
                        } else {
                            partialRowResults[idx].nextToken = null;
                        }
                        if (rowResult.getRow() != null) {
                            try {
                                if (partialRowResults[idx].primaryKey == null) {
                                    partialRowResults[idx].primaryKey = rowResult.getRow().getPrimaryKey();
                                    partialRowResults[idx].columns = new ArrayList<Column>();
                                }
                                partialRowResults[idx].columns.addAll(Arrays.asList(rowResult.getRow().getColumns()));
                            } catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                    } else {
                        partialRowResults[idx].error = rowResult.getError();
                    }
                    idx++;
                }
            }
        }
        if (requestIds == null) {
            requestIds = new ArrayList<String>();
        }
        requestIds.add(result.getRequestId());
    }

    void buildNextRequest() {
        nextRequest = new BatchGetRowRequest();
        for (Map.Entry<String, PartialRowResult[]> entry : tableToResults.entrySet()) {
            String tableName = entry.getKey();
            PartialRowResult[] partialRowResults = entry.getValue();
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            for (int i = 0; i < partialRowResults.length; i++) {
                if (!partialRowResults[i].isComplete()) {
                    request.getCriteria(tableName).copyTo(multiRowQueryCriteria);
                    multiRowQueryCriteria.addRow(request.getPrimaryKey(tableName, i), partialRowResults[i].nextToken);
                }
            }
            if (!multiRowQueryCriteria.isEmpty()) {
                nextRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);
            }
        }
    }

    void sendNextRequest() {
        buildNextRequest();
        retry = retry.clone();
        launcher.fire(nextRequest, this);
    }

    BatchGetRowResponse buildFinalResult() {
        BatchGetRowResponse batchGetRowResponse = new BatchGetRowResponse(new Response(requestIds.get(0)));
        for (Map.Entry<String, PartialRowResult[]> entry : tableToResults.entrySet()) {
            String tableName = entry.getKey();
            PartialRowResult[] partialRowResults = entry.getValue();
            for (int i = 0; i < partialRowResults.length; i++) {
                Preconditions.checkArgument(partialRowResults[i].isComplete());
                if (partialRowResults[i].error != null) {
                    BatchGetRowResponse.RowResult rowResult =
                            new BatchGetRowResponse.RowResult(tableName, partialRowResults[i].error, i);
                    batchGetRowResponse.addResult(rowResult);
                } else {
                    Row row = null;
                    if (partialRowResults[i].primaryKey != null) {
                        row = new Row(partialRowResults[i].primaryKey, partialRowResults[i].columns);
                    }
                    BatchGetRowResponse.RowResult rowResult =
                            new BatchGetRowResponse.RowResult(tableName, row, partialRowResults[i].consumedCapacity, i);
                    batchGetRowResponse.addResult(rowResult);
                }
            }
        }
        return batchGetRowResponse;
    }
}
