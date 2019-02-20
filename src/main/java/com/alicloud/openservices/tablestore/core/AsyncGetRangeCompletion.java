package com.alicloud.openservices.tablestore.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;

public class AsyncGetRangeCompletion
        extends AsyncCompletion<GetRangeRequest, GetRangeResponse> {

    GetRangeRequest nextRequest;
    List<Row> completeRows = new ArrayList<Row>();
    PrimaryKey nextPrimaryKey;
    PrimaryKey lastRowPK;
    List<Column> lastRowColumns;
    CapacityUnit capacityUnit;
    List<String> requestIds;

    public AsyncGetRangeCompletion(
            OperationLauncher<GetRangeRequest, GetRangeResponse> launcher,
            GetRangeRequest request, TraceLogger tracer,
            ExecutorService callbackExecutor,
            RetryStrategy retry, ScheduledExecutorService retryExecutor)
    {
        super(launcher, request, tracer, callbackExecutor, retry, retryExecutor);
    }

    @Override
    public void completed(GetRangeResponse result) {
        result.setTraceId(tracer.getTraceId());
        LogUtil.logOnCompleted(tracer, retry, result.getRequestId());
        tracer.printLog();

        // 终止条件: 没有nextToken 或 行数多于1行
        if (!result.hasNextToken() || result.getRows().size() > 1) {
            // 如果不是第一次请求, 需要与已有数据合并
            if (nextRequest != null) {
                handleResult(result);
                onCompleted(request, buildFinalResult());
            } else {
                // 如果最后一行不完整, 舍弃最后一行
                if (result.getRows().size() > 1) {
                    Row lastRow = result.getRows().get(result.getRows().size() - 1);
                    if (result.hasNextToken() && lastRow.getPrimaryKey().equals(result.getNextStartPrimaryKey())) {
                        result.getRows().remove(result.getRows().size() - 1);
                        result.setNextToken(null);
                    }
                }
                onCompleted(request, result);
            }
        } else {
            handleResult(result);
            sendNextRequest(result.getNextStartPrimaryKey(), result.getNextToken());
        }
    }

    void buildNextRequest(PrimaryKey nextStartPrimaryKey, byte[] token) {
        if (nextRequest == null) {
            RangeRowQueryCriteria originCriteria = request.getRangeRowQueryCriteria();
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(originCriteria.getTableName());
            rangeRowQueryCriteria.setDirection(originCriteria.getDirection());
            rangeRowQueryCriteria.setLimit(originCriteria.getLimit());
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(originCriteria.getExclusiveEndPrimaryKey());
            originCriteria.copyTo(rangeRowQueryCriteria);
            nextRequest = new GetRangeRequest(rangeRowQueryCriteria);
        }
        nextRequest.getRangeRowQueryCriteria().setInclusiveStartPrimaryKey(nextStartPrimaryKey);
        nextRequest.getRangeRowQueryCriteria().setToken(token);
    }

    void sendNextRequest(PrimaryKey nextStartPrimaryKey, byte[] token) {
        buildNextRequest(nextStartPrimaryKey, token);
        retry = retry.clone();
        launcher.fire(nextRequest, this);
    }

    void mergeLastRow(Row row, boolean complete) {
        Preconditions.checkArgument(lastRowPK != null);
        if (row != null) {
            lastRowColumns.addAll(Arrays.asList(row.getColumns()));
        }
        if (complete) {
            completeRows.add(new Row(lastRowPK, lastRowColumns));
            lastRowPK = null;
            lastRowColumns = new ArrayList<Column>();
        }
    }

    void handleResult(GetRangeResponse result) {
        if (result.getRows().size() > 0) {
            List<Row> rows = result.getRows();
            // 处理已经存在的不完整的一行
            if (lastRowPK != null) {
                if (rows.get(0).getPrimaryKey().equals(lastRowPK)) {
                    mergeLastRow(rows.get(0), !lastRowPK.equals(result.getNextStartPrimaryKey()));
                    rows.remove(0);
                } else {
                    mergeLastRow(null, true);
                }
            }
            // 有多余的行时
            if (rows.size() > 0) {
                // 增加完整的行
                for (int i = 0; i < rows.size() - 1; i++) {
                    completeRows.add(rows.get(i));
                }
                // 处理最后一行
                PrimaryKey resultLastRowPK = rows.get(rows.size() - 1).getPrimaryKey();
                if (result.hasNextToken() && resultLastRowPK.equals(result.getNextStartPrimaryKey())) {
                    lastRowPK = resultLastRowPK;
                    if (lastRowColumns == null) {
                        lastRowColumns = new ArrayList<Column>();
                    }
                    lastRowColumns.addAll(Arrays.asList(rows.get(rows.size() - 1).getColumns()));
                } else {
                    completeRows.add(rows.get(rows.size() - 1));
                }
            }
        }
        nextPrimaryKey = result.getNextStartPrimaryKey();
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

    GetRangeResponse buildFinalResult() {
        GetRangeResponse getRangeResponse = new GetRangeResponse(new Response(requestIds.get(0)), new ConsumedCapacity(capacityUnit));
        getRangeResponse.setRows(completeRows);
        getRangeResponse.setNextStartPrimaryKey(nextPrimaryKey);
        return getRangeResponse;
    }
}
