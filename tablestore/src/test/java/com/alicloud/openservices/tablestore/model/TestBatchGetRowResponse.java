package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBatchGetRowResponse {

    @Test
    public void testEmptyResult() {
        BatchGetRowResponse result = new BatchGetRowResponse(new Response());

        assertTrue(result.isAllSucceed());
        assertTrue(result.getFailedRows().isEmpty());
        assertTrue(result.getBatchGetRowResult("MyTable") == null);
        assertTrue(result.getTableToRowsResult().isEmpty());

        List<BatchGetRowResponse.RowResult> succeed = new ArrayList<BatchGetRowResponse.RowResult>();
        List<BatchGetRowResponse.RowResult> failed = new ArrayList<BatchGetRowResponse.RowResult>();
        result.getResult(succeed, failed);
        assertTrue(succeed.isEmpty());
        assertTrue(failed.isEmpty());
        assertTrue(result.getFailedRows().isEmpty());
        assertTrue(result.getSucceedRows().isEmpty());
    }

    @Test
    public void testResult_AllSucceed() {
        BatchGetRowResponse result = new BatchGetRowResponse(new Response());
        Row[] rows = new Row[10];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = TestUtil.randomRow(3, 10);
        }
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[0], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[1], null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[2], null, 2));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[3], null, 3));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[4], null, 4));

        result.addResult(new BatchGetRowResponse.RowResult("Table2", rows[5], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", rows[6], null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", rows[7], null, 2));

        result.addResult(new BatchGetRowResponse.RowResult("Table3", rows[8], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table3", rows[9], null, 1));

        assertTrue(result.isAllSucceed());
        assertTrue(result.getFailedRows().isEmpty());
        assertEquals(result.getSucceedRows().size(), 10);
        for (int i = 0; i < 10; i++) {
            assertTrue(result.getSucceedRows().get(i).isSucceed());
        }

        assertEquals(result.getTableToRowsResult().size(), 3);
        assertEquals(result.getTableToRowsResult().get("Table1").size(), 5);

        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(0).getRow(), rows[0]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(0).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(1).getRow(), rows[1]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(1).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(2).getRow(), rows[2]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(2).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(3).getRow(), rows[3]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(3).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(4).getRow(), rows[4]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(4).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table2").size(), 3);
        TestUtil.compareRow(result.getTableToRowsResult().get("Table2").get(0).getRow(), rows[5]);
        assertTrue(result.getTableToRowsResult().get("Table2").get(0).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table2").get(1).getRow(), rows[6]);
        assertTrue(result.getTableToRowsResult().get("Table2").get(1).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table2").get(2).getRow(), rows[7]);
        assertTrue(result.getTableToRowsResult().get("Table2").get(2).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table3").size(), 2);
        TestUtil.compareRow(result.getTableToRowsResult().get("Table3").get(0).getRow(), rows[8]);
        assertTrue(result.getTableToRowsResult().get("Table3").get(0).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table3").get(1).getRow(), rows[9]);
        assertTrue(result.getTableToRowsResult().get("Table3").get(1).isSucceed());
    }

    @Test
    public void testResult_AllFailed() {
        BatchGetRowResponse result = new BatchGetRowResponse(new Response());

        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 2));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 3));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 4));

        result.addResult(new BatchGetRowResponse.RowResult("Table2", null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", null, 2));

        result.addResult(new BatchGetRowResponse.RowResult("Table3", null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table3", null, 1));

        assertTrue(!result.isAllSucceed());
        assertEquals(result.getFailedRows().size(), 10);
        assertEquals(result.getSucceedRows().size(), 0);

        for (BatchGetRowResponse.RowResult rowResult : result.getFailedRows()) {
            assertTrue(!rowResult.isSucceed());
        }

        assertEquals(result.getTableToRowsResult().size(), 3);
        assertEquals(result.getTableToRowsResult().get("Table1").size(), 5);

        assertTrue(!result.getTableToRowsResult().get("Table1").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(1).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(2).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(3).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(4).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table2").size(), 3);
        assertTrue(!result.getTableToRowsResult().get("Table2").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table2").get(1).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table2").get(2).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table3").size(), 2);
        assertTrue(!result.getTableToRowsResult().get("Table3").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table3").get(1).isSucceed());
    }

    @Test
    public void testResult_Mix() {
        BatchGetRowResponse result = new BatchGetRowResponse(new Response());
        Row[] rows = new Row[6];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = TestUtil.randomRow(3, 10);
        }
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[0], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[1], null, 2));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", null, 3));
        result.addResult(new BatchGetRowResponse.RowResult("Table1", rows[2], null, 4));

        result.addResult(new BatchGetRowResponse.RowResult("Table2", rows[3], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", null, 1));
        result.addResult(new BatchGetRowResponse.RowResult("Table2", rows[4], null, 2));

        result.addResult(new BatchGetRowResponse.RowResult("Table3", rows[5], null, 0));
        result.addResult(new BatchGetRowResponse.RowResult("Table3", null, 1));

        assertTrue(!result.isAllSucceed());
        assertEquals(result.getFailedRows().size(), 4);
        assertEquals(result.getSucceedRows().size(), 6);
        for (int i = 0; i < 6; i++) {
            assertTrue(result.getSucceedRows().get(i).isSucceed());
        }

        for (int i = 0; i < 4; i++) {
            assertTrue(!result.getFailedRows().get(i).isSucceed());
        }

        assertEquals(result.getTableToRowsResult().size(), 3);
        assertEquals(result.getTableToRowsResult().get("Table1").size(), 5);

        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(0).getRow(), rows[0]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(1).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(2).getRow(), rows[1]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(2).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table1").get(3).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table1").get(4).getRow(), rows[2]);
        assertTrue(result.getTableToRowsResult().get("Table1").get(4).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table2").size(), 3);
        TestUtil.compareRow(result.getTableToRowsResult().get("Table2").get(0).getRow(), rows[3]);
        assertTrue(result.getTableToRowsResult().get("Table2").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table2").get(1).isSucceed());
        TestUtil.compareRow(result.getTableToRowsResult().get("Table2").get(2).getRow(), rows[4]);
        assertTrue(result.getTableToRowsResult().get("Table2").get(2).isSucceed());

        assertEquals(result.getTableToRowsResult().get("Table3").size(), 2);
        TestUtil.compareRow(result.getTableToRowsResult().get("Table3").get(0).getRow(), rows[5]);
        assertTrue(result.getTableToRowsResult().get("Table3").get(0).isSucceed());
        assertTrue(!result.getTableToRowsResult().get("Table3").get(1).isSucceed());
    }
}
