package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.lang.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBatchWriteRowResponse {

    @Test
    public void testEmptyResult() {
        BatchWriteRowResponse result = new BatchWriteRowResponse(new Response());

        assertTrue(result.getRowStatus().isEmpty());
        assertTrue(result.getSucceedRows().isEmpty());

        assertTrue(result.getRowStatus("Table") == null);
    }

    private void compareStatus(List<BatchWriteRowResponse.RowResult> l, Map<String, List<BatchWriteRowResponse.RowResult>> m) {
        Map<String, List<BatchWriteRowResponse.RowResult>> tmp = new HashMap<String, List<BatchWriteRowResponse.RowResult>>();
        for (BatchWriteRowResponse.RowResult rowResult : l) {
            List<BatchWriteRowResponse.RowResult> tl =  tmp.get(rowResult.getTableName());
            if (tl == null) {
                tl = new ArrayList<BatchWriteRowResponse.RowResult>();
                tmp.put(rowResult.getTableName(), tl);
            }

            tl.add(rowResult);
        }

        for (String key : tmp.keySet()) {
            List<BatchWriteRowResponse.RowResult> tl1 =  tmp.get(key);
            List<BatchWriteRowResponse.RowResult> tl2 =  m.get(key);
            compareStatusSorted(tl1, tl2);
        }
    }

    private Map<String, List<BatchWriteRowResponse.RowResult>> toMap(List<BatchWriteRowResponse.RowResult> l) {
        Map<String, List<BatchWriteRowResponse.RowResult>> m = new HashMap<String, List<BatchWriteRowResponse.RowResult>>();
        for (BatchWriteRowResponse.RowResult rowResult : l) {
            List<BatchWriteRowResponse.RowResult> tmp = m.get(rowResult.getTableName());
            if (tmp == null) {
                tmp = new ArrayList<BatchWriteRowResponse.RowResult>();
                m.put(rowResult.getTableName(), tmp);
            }
            tmp.add(rowResult);
        }
        return m;
    }

    private void compareStatusUnSorted(List<BatchWriteRowResponse.RowResult> tl1, List<BatchWriteRowResponse.RowResult> tl2) {
        assertEquals(tl1.size(), tl2.size());
        Map<String, List<BatchWriteRowResponse.RowResult>> m1 = toMap(tl1);
        Map<String, List<BatchWriteRowResponse.RowResult>> m2 = toMap(tl2);

        assertEquals(m1.size(), m2.size());

        for (String key : m1.keySet()) {
            compareStatusSorted(m1.get(key), m2.get(key));
        }
    }

    private void compareStatusSorted(List<BatchWriteRowResponse.RowResult> tl1, List<BatchWriteRowResponse.RowResult> tl2) {
        assertEquals(tl1.size(), tl2.size());

        for (int i = 0; i < tl1.size(); i++) {
            BatchWriteRowResponse.RowResult r1 = tl1.get(i);
            BatchWriteRowResponse.RowResult r2 = tl2.get(i);

            assertEquals(r1.getTableName(), r2.getTableName());
            assertEquals(r1.isSucceed(), r2.isSucceed());
            assertEquals(r1.getIndex(), r2.getIndex());
        }
    }

    private void compareStatusMixed(List<BatchWriteRowResponse.RowResult> succeedRows, List<BatchWriteRowResponse.RowResult> failedRows, List<BatchWriteRowResponse.RowResult> tl2) {
        List<BatchWriteRowResponse.RowResult> s = new ArrayList<BatchWriteRowResponse.RowResult>();
        List<BatchWriteRowResponse.RowResult> f = new ArrayList<BatchWriteRowResponse.RowResult>();
        for (BatchWriteRowResponse.RowResult rowResult : tl2) {
            if (rowResult.isSucceed()) {
                s.add(rowResult);
            } else {
                f.add(rowResult);
            }
        }
        compareStatusSorted(succeedRows, s);
        compareStatusSorted(failedRows, f);
    }

    @Test
    public void testResult_AllSucceed() {
        BatchWriteRowResponse result = new BatchWriteRowResponse(new Response());
        String tableName1 = "TableName1";
        String tableName2 = "TableName2";
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName1, null, new ConsumedCapacity(new CapacityUnit(1, 1)), 0));
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName1, null, new ConsumedCapacity(new CapacityUnit(1, 1)), 1));

        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName2, null, new ConsumedCapacity(new CapacityUnit(1, 1)), 0));
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName2, null, new ConsumedCapacity(new CapacityUnit(1, 1)), 1));

        assertTrue(result.getFailedRows().isEmpty());

        assertEquals(result.getSucceedRows().size(), 4);
        compareStatus(result.getSucceedRows(), result.getRowStatus());
    }

    @Test
    public void testResult_AllFailed() {
        BatchWriteRowResponse result = new BatchWriteRowResponse(new Response());
        String tableName1 = "TableName1";
        String tableName2 = "TableName2";
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName1, null, new Error("", ""), 0));
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName1, null, new Error("", ""), 1));

        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName2, null, new Error("", ""), 0));
        result.addRowResult(new BatchWriteRowResponse.RowResult(tableName2, null, new Error("", ""), 1));

        assertTrue(result.getSucceedRows().isEmpty());

        assertEquals(result.getFailedRows().size(), 4);
        compareStatus(result.getFailedRows(), result.getRowStatus());
    }

    @Test
    public void testResult_Mix() {
        BatchWriteRowResponse result = new BatchWriteRowResponse(new Response());
        int tableCount = 10;
        List<BatchWriteRowResponse.RowResult> rowPutSucceedResults = new ArrayList<BatchWriteRowResponse.RowResult>();
        List<BatchWriteRowResponse.RowResult> rowPutFailedResults = new ArrayList<BatchWriteRowResponse.RowResult>();

        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 999; i++) {
            if (random.nextBoolean()) {
                rowPutFailedResults.add(new BatchWriteRowResponse.RowResult("TableName" + random.nextInt(tableCount), null, new Error("", ""), random.nextInt(100)));
            } else  {
                rowPutSucceedResults.add(new BatchWriteRowResponse.RowResult("TableName" + random.nextInt(tableCount), null, new ConsumedCapacity(new CapacityUnit(1, 1)), random.nextInt(100)));
            }
        }

        for (BatchWriteRowResponse.RowResult rowResult : rowPutSucceedResults) {
            result.addRowResult(rowResult);
        }
        for (BatchWriteRowResponse.RowResult rowResult : rowPutFailedResults) {
            result.addRowResult(rowResult);
        }

        assertEquals(result.getRowStatus().size(), tableCount);

        for (int i = 0; i < tableCount; i++) {
            String tableName = "TableName" + i;
            assertTrue(result.getRowStatus(tableName) != null);

            compareStatusMixed(toMap(rowPutSucceedResults).get(tableName), toMap(rowPutFailedResults).get(tableName), result.getRowStatus(tableName));
        }

        compareStatusUnSorted(rowPutSucceedResults, result.getSucceedRows());
        compareStatusUnSorted(rowPutFailedResults, result.getFailedRows());

        List<BatchWriteRowResponse.RowResult> succeed = new ArrayList<BatchWriteRowResponse.RowResult>();
        List<BatchWriteRowResponse.RowResult> failed = new ArrayList<BatchWriteRowResponse.RowResult>();
        result.getResult(succeed, failed);
        compareStatusUnSorted(rowPutSucceedResults, succeed);
        compareStatusUnSorted(rowPutFailedResults, failed);
    }
}
