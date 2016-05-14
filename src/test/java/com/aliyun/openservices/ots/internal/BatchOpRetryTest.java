package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.model.Error;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class BatchOpRetryTest {

    private RowPrimaryKey getPrimaryKey(int key) {
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        primaryKey.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(key));
        return primaryKey;
    }

    @Test
    public void testMergeBatchWriteRowResult() {
        boolean[][] isSuccess = new boolean[10][100];

        Random random = new Random();
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        for (int tid = 0; tid < 10; tid++) {
            if (tid % 3 == 0) {
                for (int rid = 0; rid < 100; rid++) {
                    RowPutChange rowPutChange = new RowPutChange("table-" + tid);
                    rowPutChange.setPrimaryKey(getPrimaryKey(rid));
                    request.addRowPutChange(rowPutChange);
                    isSuccess[tid][rid] = random.nextBoolean();
                }
            }
            if (tid % 3 == 1) {
                for (int rid = 0; rid < 100; rid++) {
                    RowUpdateChange rowUpdateChange = new RowUpdateChange("table-" + tid);
                    rowUpdateChange.setPrimaryKey(getPrimaryKey(rid));
                    request.addRowUpdateChange(rowUpdateChange);
                    isSuccess[tid][rid] = random.nextBoolean();
                }
            }
            if (tid % 3 == 2) {
                for (int rid = 0; rid < 100; rid++) {
                    RowDeleteChange rowDeleteChange = new RowDeleteChange("table-" + tid);
                    rowDeleteChange.setPrimaryKey(getPrimaryKey(rid));
                    request.addRowDeleteChange(rowDeleteChange);
                    isSuccess[tid][rid] = random.nextBoolean();
                }
            }
        }

        OTSResult meta = new OTSResult();
        meta.setRequestID("");
        meta.setRequestID("");
        BatchWriteRowResult result = new BatchWriteRowResult(meta);

        for (int tid = 0; tid < 10; tid++) {
            for (int rid = 0; rid < 100; rid++) {
                int rowIdx = rid;
                if (isSuccess[tid][rowIdx]) {
                    ConsumedCapacity capacity = new ConsumedCapacity();
                    capacity.setCapacityUnit(new CapacityUnit(0, 1));
                    if (tid % 3 == 0) {
                        result.addPutRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    }
                    if (tid % 3 == 1) {
                        result.addUpdateRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    }
                    if (tid % 3 == 2) {
                        result.addDeleteRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    }
                } else {
                    if (tid % 3 == 0) {
                        result.addPutRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                    if (tid % 3 == 1) {
                        result.addUpdateRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                    if (tid % 3 == 2) {
                        result.addDeleteRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                }
            }
        }

        BatchWriteRowRequest newRequest = request.createRequestForRetry(result.getFailedRowsOfPut(), result.getFailedRowsOfUpdate(), result.getFailedRowsOfDelete());
        BatchWriteRowResult newResult = new BatchWriteRowResult(meta);

        for (int tid = 0; tid < 10; tid++) {
            if (tid % 3 == 0) {
                int rowCount = newRequest.getRowPutChange().get("table-" + tid).size();
                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    RowPrimaryKey pk = newRequest.getRowPutChange("table-" + tid, rowIdx).getRowPrimaryKey();
                    int key = (int)pk.getPrimaryKey().get("pk").asLong();
                    assertEquals(false, isSuccess[tid][key]);
                    if (random.nextBoolean()) {
                        isSuccess[tid][key] = true;
                        ConsumedCapacity capacity = new ConsumedCapacity();
                        capacity.setCapacityUnit(new CapacityUnit(0, 1));
                        newResult.addPutRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    } else {
                        newResult.addPutRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                }
            }
            if (tid % 3 == 1) {
                int rowCount = newRequest.getRowUpdateChange().get("table-" + tid).size();
                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    RowPrimaryKey pk = newRequest.getRowUpdateChange("table-" + tid, rowIdx).getRowPrimaryKey();
                    int key = (int)pk.getPrimaryKey().get("pk").asLong();
                    assertEquals(false, isSuccess[tid][key]);
                    if (random.nextBoolean()) {
                        isSuccess[tid][key] = true;
                        ConsumedCapacity capacity = new ConsumedCapacity();
                        capacity.setCapacityUnit(new CapacityUnit(0, 1));
                        newResult.addUpdateRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    } else {
                        newResult.addUpdateRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                }
            }
            if (tid % 3 == 2) {
                int rowCount = newRequest.getRowDeleteChange().get("table-" + tid).size();
                for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    RowPrimaryKey pk = newRequest.getRowDeleteChange("table-" + tid, rowIdx).getRowPrimaryKey();
                    int key = (int)pk.getPrimaryKey().get("pk").asLong();
                    assertEquals(false, isSuccess[tid][key]);
                    if (random.nextBoolean()) {
                        isSuccess[tid][key] = true;
                        ConsumedCapacity capacity = new ConsumedCapacity();
                        capacity.setCapacityUnit(new CapacityUnit(0, 1));
                        newResult.addDeleteRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, capacity, rowIdx));
                    } else {
                        newResult.addDeleteRowResult(new BatchWriteRowResult.RowStatus("table-" + tid, new Error("", ""), rowIdx));
                    }
                }
            }
        }

        BatchWriteRowExecutionContext ec = new BatchWriteRowExecutionContext(null, null, new OTSTraceLogger("", 1000), null, null);
        BatchWriteRowAsyncResponseConsumer consumer = new BatchWriteRowAsyncResponseConsumer(null, ec);

        BatchWriteRowResult mergedResult = consumer.mergeResult(result, newResult);
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getFailedRowsOfPut()) {
            assertEquals(false, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getSucceedRowsOfPut()) {
            assertEquals(true, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getFailedRowsOfUpdate()) {
            assertEquals(false, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getSucceedRowsOfUpdate()) {
            assertEquals(true, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getFailedRowsOfDelete()) {
            assertEquals(false, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchWriteRowResult.RowStatus rowResult : mergedResult.getSucceedRowsOfDelete()) {
            assertEquals(true, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
    }

    @Test
    public void testMergeBatchGetRowResult() {

        boolean[][] isSuccess = new boolean[10][100];

        Random random = new Random();
        BatchGetRowRequest request = new BatchGetRowRequest();
        for (int tid = 0; tid < 10; tid++) {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("table-" + tid);
            for (int rid = 0; rid < 100; rid++) {
                criteria.addRow(getPrimaryKey(rid));
                isSuccess[tid][rid] = random.nextBoolean();
            }
            request.addMultiRowQueryCriteria(criteria);
        }

        OTSResult meta = new OTSResult();
        meta.setRequestID("");
        meta.setRequestID("");
        BatchGetRowResult result = new BatchGetRowResult(meta);

        for (int tid = 0; tid < 10; tid++) {
            for (int rowIdx = 0; rowIdx < 100; rowIdx++) {
                if (isSuccess[tid][rowIdx]) {
                    Row row = new Row();
                    row.addColumn("pk", ColumnValue.fromLong(rowIdx));
                    result.addResult(new BatchGetRowResult.RowStatus("table-" + tid, row,
                            new ConsumedCapacity(), rowIdx));
                } else {
                    Error error = new Error("testError", "testError");
                    result.addResult(new BatchGetRowResult.RowStatus("table-" + tid, error, rowIdx));
                }
            }
        }

        BatchGetRowRequest newRequest = request.createRequestForRetry(result.getFailedRows());
        BatchGetRowResult newResult = new BatchGetRowResult(meta);

        for (int tid = 0; tid < 10; tid++) {
            MultiRowQueryCriteria criteria = newRequest.getCriteria("table-" + tid);
            int rowCount = criteria.size();

            for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                RowPrimaryKey pk = criteria.get(rowIdx);
                int key = (int)pk.getPrimaryKey().get("pk").asLong();
                assertEquals(false, isSuccess[tid][key]);
                if (random.nextBoolean()) {
                    isSuccess[tid][key] = true;
                    Row row = new Row();
                    row.addColumn("pk", ColumnValue.fromLong(rowIdx));
                    newResult.addResult(new BatchGetRowResult.RowStatus("table-" + tid, row,
                            new ConsumedCapacity(), rowIdx));
                } else {
                    Error error = new Error("testError", "testError");
                    newResult.addResult(new BatchGetRowResult.RowStatus("table-" + tid, error, rowIdx));
                }
            }
        }

        BatchGetRowExecutionContext ec = new BatchGetRowExecutionContext(null, null, new OTSTraceLogger("", 1000), null, null);
        BatchGetRowAsyncResponseConsumer consumer = new BatchGetRowAsyncResponseConsumer(null, ec);

        BatchGetRowResult mergedResult = consumer.mergeResult(result, newResult);
        for (BatchGetRowResult.RowStatus rowResult : mergedResult.getFailedRows()) {
            assertEquals(false, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
        for (BatchGetRowResult.RowStatus rowResult : mergedResult.getSucceedRows()) {
            assertEquals(true, isSuccess[Integer.parseInt(rowResult.getTableName().substring(6))][rowResult.getIndex()]);
        }
    }

}
