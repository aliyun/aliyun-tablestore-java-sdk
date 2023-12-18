package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import org.junit.Test;

import java.lang.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

public class TestBatchWriteRowRequest {

    @Test
    public void testEmptyRequest() {
        BatchWriteRowRequest request = new BatchWriteRowRequest();

        assertTrue(request.isEmpty());
        assertTrue(request.getRowChange().isEmpty());
        assertTrue(request.getRowChange("NonExistTable", 0) == null);
    }

    @Test
    public void testCreateRequestForRetry() {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        String tableName = "MyTable";

        PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = 0; i < 100; i++) {
            pks.add(TestUtil.randomPrimaryKey(schema));
        }
        for (int i = 0; i < 100; i++) {
            RowPutChange rowChange = new RowPutChange(tableName, pks.get(i));
            rowChange.addColumn("column" + i, ColumnValue.fromString("HelloWorld"));
            request.addRowChange(rowChange);
        }
        assertEquals(request.getRowChange().size(), 1);
        assertEquals(request.getRowChange().get(tableName).size(), 100);

        List<BatchWriteRowResponse.RowResult> rowResults = new ArrayList<BatchWriteRowResponse.RowResult>();
        rowResults.add(new BatchWriteRowResponse.RowResult(tableName, null, new Error("Code", "Msg"), 10));
        rowResults.add(new BatchWriteRowResponse.RowResult(tableName, null, new Error("Code", "Msg"), 20));

        {
            BatchWriteRowRequest requestForRetry = request.createRequestForRetry(rowResults);
            assertEquals(requestForRetry.getRowChange().size(), 1);
            assertEquals(requestForRetry.getRowChange().get(tableName).size(), 2);
            assertEquals(requestForRetry.getRowChange(tableName, 0).getPrimaryKey(), pks.get(10));
            assertEquals(requestForRetry.getRowChange(tableName, 1).getPrimaryKey(), pks.get(20));
            assertFalse(requestForRetry.hasSetTransactionId());
            assertFalse(requestForRetry.isAtomicSet());
        }

        request.setAtomic(true);
        {
            BatchWriteRowRequest requestForRetry = request.createRequestForRetry(rowResults);
            assertEquals(requestForRetry.getRowChange().size(), 1);
            assertEquals(requestForRetry.getRowChange().get(tableName).size(), 2);
            assertEquals(requestForRetry.getRowChange(tableName, 0).getPrimaryKey(), pks.get(10));
            assertEquals(requestForRetry.getRowChange(tableName, 1).getPrimaryKey(), pks.get(20));
            assertFalse(requestForRetry.hasSetTransactionId());
            assertTrue(requestForRetry.isAtomicSet());
            assertTrue(requestForRetry.isAtomic());
        }
        request.setAtomic(false);
        {
            BatchWriteRowRequest requestForRetry = request.createRequestForRetry(rowResults);
            assertEquals(requestForRetry.getRowChange().size(), 1);
            assertEquals(requestForRetry.getRowChange().get(tableName).size(), 2);
            assertEquals(requestForRetry.getRowChange(tableName, 0).getPrimaryKey(), pks.get(10));
            assertEquals(requestForRetry.getRowChange(tableName, 1).getPrimaryKey(), pks.get(20));
            assertFalse(requestForRetry.hasSetTransactionId());
            assertTrue(requestForRetry.isAtomicSet());
            assertFalse(requestForRetry.isAtomic());
        }
        request.setTransactionId("xxx");
        {
            BatchWriteRowRequest requestForRetry = request.createRequestForRetry(rowResults);
            assertEquals(requestForRetry.getRowChange().size(), 1);
            assertEquals(requestForRetry.getRowChange().get(tableName).size(), 2);
            assertEquals(requestForRetry.getRowChange(tableName, 0).getPrimaryKey(), pks.get(10));
            assertEquals(requestForRetry.getRowChange(tableName, 1).getPrimaryKey(), pks.get(20));
            assertTrue(requestForRetry.hasSetTransactionId());
            assertEquals(requestForRetry.getTransactionId(), "xxx");
            assertTrue(requestForRetry.isAtomicSet());
            assertFalse(requestForRetry.isAtomic());
        }
    }

    @Test
    public void testRequest_RowChange_WithOneTable() {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        String tableName = "MyTable";

        PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = 0; i < 100; i++) {
            pks.add(TestUtil.randomPrimaryKey(schema));
        }

        for (int i = 0; i < 100; i++) {
            RowPutChange rowChange = new RowPutChange(tableName, pks.get(i));
            rowChange.addColumn("column" + i, ColumnValue.fromString("HelloWorld"));
            request.addRowChange(rowChange);
        }

        assertEquals(request.getRowChange().size(), 1);
        assertEquals(request.getRowChange().get(tableName).size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(request.getRowChange().get(tableName).get(i).getPrimaryKey(), pks.get(i));
            assertEquals(((RowPutChange)request.getRowChange().get(tableName).get(i)).getColumnsToPut().size(), 1);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName).get(i)).getColumnsToPut().get(0).getName(), "column" + i);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName).get(i)).getColumnsToPut().get(0).getValue(), ColumnValue.fromString("HelloWorld"));
        }

        assertTrue(request.getRowChange().get("NonExistTable") == null);
        assertTrue(request.getRowChange(tableName, 101) == null);
    }

    @Test
    public void testRequest_RowPutChange_WithMultiTable() {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        String tableName1 = "MyTable1";
        String tableName2 = "MyTable2";

        PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = 0; i < 100; i++) {
            pks.add(TestUtil.randomPrimaryKey(schema));
        }

        for (int i = 0; i < 100; i++) {
            RowPutChange rowChange = new RowPutChange(tableName1, pks.get(i));
            rowChange.addColumn("column1_" + i, ColumnValue.fromString("HelloWorld"));
            request.addRowChange(rowChange);

            rowChange = new RowPutChange(tableName2, pks.get(i));
            rowChange.addColumn("column2_" + i, ColumnValue.fromString("HelloWorld"));
            request.addRowChange(rowChange);
        }

        assertEquals(request.getRowChange().size(), 2);
        assertEquals(request.getRowChange().get(tableName1).size(), 100);
        assertEquals(request.getRowChange().get(tableName2).size(), 100);

        for (int i = 0; i < 100; i++) {
            assertEquals(request.getRowChange().get(tableName1).get(i).getPrimaryKey(), pks.get(i));
            assertEquals(((RowPutChange)request.getRowChange().get(tableName1).get(i)).getColumnsToPut().size(), 1);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName1).get(i)).getColumnsToPut().get(0).getName(), "column1_" + i);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName1).get(i)).getColumnsToPut().get(0).getValue(), ColumnValue.fromString("HelloWorld"));

            assertEquals(request.getRowChange().get(tableName2).get(i).getPrimaryKey(), pks.get(i));
            assertEquals(((RowPutChange)request.getRowChange().get(tableName2).get(i)).getColumnsToPut().size(), 1);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName2).get(i)).getColumnsToPut().get(0).getName(), "column2_" + i);
            assertEquals(((RowPutChange)request.getRowChange().get(tableName2).get(i)).getColumnsToPut().get(0).getValue(), ColumnValue.fromString("HelloWorld"));
        }
    }

    @Test
    public void testRequest_MixOperation_WithMultiTable() {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        String tableName1 = "MyTable1";
        String tableName2 = "MyTable2";

        PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);
        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = 0; i < 100; i++) {
            pks.add(TestUtil.randomPrimaryKey(schema));
        }

        Random random = new Random();
        List<Integer> types = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++) {
            int opType = random.nextInt(3);
            types.add(opType);
            switch (opType) {
                case 0: {
                    RowPutChange rowChange = new RowPutChange(tableName1, pks.get(i));
                    rowChange.addColumn("column1_" + i, ColumnValue.fromString("HelloWorld"));
                    request.addRowChange(rowChange);

                    rowChange = new RowPutChange(tableName2, pks.get(i));
                    rowChange.addColumn("column2_" + i, ColumnValue.fromString("HelloWorld"));
                    request.addRowChange(rowChange);
                    break;
                }
                case 1: {
                    RowUpdateChange rowChange = new RowUpdateChange(tableName1, pks.get(i));
                    rowChange.put("column1_" + i, ColumnValue.fromString("HelloWorld"));
                    request.addRowChange(rowChange);

                    rowChange = new RowUpdateChange(tableName2, pks.get(i));
                    rowChange.put("column2_" + i, ColumnValue.fromString("HelloWorld"));
                    request.addRowChange(rowChange);
                    break;
                }
                case 2: {
                    RowDeleteChange rowChange = new RowDeleteChange(tableName1, pks.get(i));
                    request.addRowChange(rowChange);
                    rowChange = new RowDeleteChange(tableName2, pks.get(i));
                    request.addRowChange(rowChange);
                    break;
                }
                default: {
                    throw new RuntimeException();
                }
            }
        }

        assertEquals(request.getRowChange().size(), 2);
        assertEquals(request.getRowChange().get(tableName1).size(), 100);
        assertEquals(request.getRowChange().get(tableName2).size(), 100);

        for (int i = 0; i < 100; i++) {
            int opType = types.get(i);
            switch (opType) {
                case 0: {
                    assertEquals(request.getRowChange().get(tableName1).get(i).getPrimaryKey(), pks.get(i));
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName1).get(i)).getColumnsToPut().size(), 1);
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName1).get(i)).getColumnsToPut().get(0).getName(), "column1_" + i);
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName1).get(i)).getColumnsToPut().get(0).getValue(), ColumnValue.fromString("HelloWorld"));

                    assertEquals(request.getRowChange().get(tableName2).get(i).getPrimaryKey(), pks.get(i));
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName2).get(i)).getColumnsToPut().size(), 1);
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName2).get(i)).getColumnsToPut().get(0).getName(), "column2_" + i);
                    assertEquals(((RowPutChange) request.getRowChange().get(tableName2).get(i)).getColumnsToPut().get(0).getValue(), ColumnValue.fromString("HelloWorld"));
                    break;
                }
                case 1: {
                    assertEquals(request.getRowChange().get(tableName1).get(i).getPrimaryKey(), pks.get(i));
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName1).get(i)).getColumnsToUpdate().size(), 1);
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName1).get(i)).getColumnsToUpdate().get(0).getFirst().getName(), "column1_" + i);
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName1).get(i)).getColumnsToUpdate().get(0).getFirst().getValue(), ColumnValue.fromString("HelloWorld"));

                    assertEquals(request.getRowChange().get(tableName2).get(i).getPrimaryKey(), pks.get(i));
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName2).get(i)).getColumnsToUpdate().size(), 1);
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName2).get(i)).getColumnsToUpdate().get(0).getFirst().getName(), "column2_" + i);
                    assertEquals(((RowUpdateChange) request.getRowChange().get(tableName2).get(i)).getColumnsToUpdate().get(0).getFirst().getValue(), ColumnValue.fromString("HelloWorld"));
                    break;
                }
                case 2: {
                    assertEquals(((RowDeleteChange) request.getRowChange().get(tableName1).get(i)).getPrimaryKey(), pks.get(i));
                    assertEquals(((RowDeleteChange) request.getRowChange().get(tableName2).get(i)).getPrimaryKey(), pks.get(i));
                    break;
                }
                default: {
                    throw new RuntimeException();
                }
            }
        }
    }

}
