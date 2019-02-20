package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class AsyncGetRowCompletion
        extends AsyncCompletion<GetRowRequest, GetRowResponse> {

    GetRowRequest nextRequest;
    PrimaryKey primaryKey;
    List<Column> columns;
    CapacityUnit capacityUnit;
    List<String> requestIds;

    public AsyncGetRowCompletion(
            OperationLauncher<GetRowRequest, GetRowResponse> launcher,
            GetRowRequest request, TraceLogger tracer,
            ExecutorService callbackExecutor,
            RetryStrategy retry, ScheduledExecutorService retryExecutor)
    {
        super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
    }

    @Override
    public void completed(GetRowResponse result) {
        result.setTraceId(tracer.getTraceId());
        LogUtil.logOnCompleted(tracer, retry, result.getRequestId());
        tracer.printLog();

        // 终止条件: 没有nextToken
        if (!result.hasNextToken()) {
            // 如果不是第一次请求, 需要与已有数据合并
            if (nextRequest != null) {
                handleResult(result);
                onCompleted(request, buildFinalResult());
            } else {
                onCompleted(request, result);
            }
        } else {
            handleResult(result);
            sendNextRequest(result.getNextToken());
        }
    }

    void handleResult(GetRowResponse result) {
        if (result.getRow() != null) {
            if (primaryKey == null) {
                primaryKey = result.getRow().getPrimaryKey();
            }
            if (columns == null) {
                columns = new ArrayList<Column>();
            }
            columns.addAll(Arrays.asList(result.getRow().getColumns()));
        }
        if (capacityUnit == null) {
            capacityUnit = new CapacityUnit(0, 0);
        }
        if (requestIds == null) {
            requestIds = new ArrayList<String>();
        }
        capacityUnit.setReadCapacityUnit(capacityUnit.getReadCapacityUnit() + result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
        capacityUnit.setWriteCapacityUnit(capacityUnit.getWriteCapacityUnit() + result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
        requestIds.add(result.getRequestId());
    }

    void buildNextRequest(byte[] token) {
        if (nextRequest == null) {
            SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(
                    request.getRowQueryCriteria().getTableName(), request.getRowQueryCriteria().getPrimaryKey());
            request.getRowQueryCriteria().copyTo(singleRowQueryCriteria);
            nextRequest = new GetRowRequest(singleRowQueryCriteria);
        }
        nextRequest.getRowQueryCriteria().setToken(token);
    }

    void sendNextRequest(byte[] token) {
        buildNextRequest(token);
        retry = retry.clone();
        launcher.fire(nextRequest, this);
    }

    GetRowResponse buildFinalResult() {
        Row row = null;
        if (primaryKey != null) {
            row = new Row(primaryKey, columns);
        }
        return new GetRowResponse(new Response(this.requestIds.get(0)), row, new ConsumedCapacity(capacityUnit));
    }
}
