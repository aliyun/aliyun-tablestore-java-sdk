package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestBatchGetRowRequest {

    @Test
    public void testEmptyRequest() {
        BatchGetRowRequest request = new BatchGetRowRequest();

        assertTrue(request.isEmpty());
        assertTrue(request.getCriteriasByTable().isEmpty());
        assertTrue(request.getCriteria("NonExistTable") == null);
        assertTrue(request.getPrimaryKey("NonExistTable", 2) == null);
    }

    private void checkTable(BatchGetRowRequest request, MultiRowQueryCriteria criteria, String tableName) {
        assertTrue(request.getCriteria(tableName) != null);
        MultiRowQueryCriteria target = request.getCriteria(tableName);

        List<PrimaryKey> pks = criteria.getRowKeys();
        assertEquals(target.getRowKeys().size(), pks.size());
        for (int i = 0; i < pks.size(); i++) {
            assertEquals(target.get(i), criteria.get(i));
            assertEquals(request.getPrimaryKey(tableName, i), criteria.get(i));
        }
    }

    @Test
    public void testRequest_WithOneTable() {
        BatchGetRowRequest request = new BatchGetRowRequest();
        String tableName = "MyTable";
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);

        List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
        for (int i = 0; i < 100; i++) {
            pks.add(TestUtil.randomPrimaryKey(schema));
        }

        for (PrimaryKey pk : pks) {
            criteria.addRow(pk);
        }

        request.addMultiRowQueryCriteria(criteria);
        assertEquals(request.getCriteriasByTable().size(), 1);

        checkTable(request, criteria, tableName);
    }

    @Test
    public void testRequest_WithMultiTable() {
        BatchGetRowRequest request = new BatchGetRowRequest();
        String tableName1 = "MyTable1";
        MultiRowQueryCriteria criteria1 = new MultiRowQueryCriteria(tableName1);
        String tableName2 = "MyTable2";
        MultiRowQueryCriteria criteria2 = new MultiRowQueryCriteria(tableName2);
        {
            PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);

            List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
            for (int i = 0; i < 100; i++) {
                pks.add(TestUtil.randomPrimaryKey(schema));
            }

            for (PrimaryKey pk : pks) {
                criteria1.addRow(pk);
            }

            request.addMultiRowQueryCriteria(criteria1);
        }
        {
            PrimaryKeySchema[] schema = TestUtil.randomPrimaryKeySchema(5);

            List<PrimaryKey> pks = new ArrayList<PrimaryKey>();
            for (int i = 0; i < 50; i++) {
                pks.add(TestUtil.randomPrimaryKey(schema));
            }

            for (PrimaryKey pk : pks) {
                criteria2.addRow(pk);
            }

            request.addMultiRowQueryCriteria(criteria2);
        }

        assertEquals(request.getCriteriasByTable().size(), 2);
        checkTable(request, criteria1, tableName1);
        checkTable(request, criteria2, tableName2);
    }

    @Test
    public void testRequest_InvalidParameter() {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("MyTable");
        try {
            // add empty parameter
            request.addMultiRowQueryCriteria(criteria);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testCreateRequestForRetry() {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria1 = new MultiRowQueryCriteria("Table1");
        criteria1.addColumnsToGet("Column1");
        criteria1.setMaxVersions(1);
        criteria1.setTimeRange(new TimeRange(1, 100));
        List<PrimaryKey> pks1 = new ArrayList<PrimaryKey>();
        pks1.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(1)).build());
        pks1.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(2)).build());
        pks1.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(3)).build());
        pks1.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(4)).build());
        pks1.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromLong(5)).build());

        for (PrimaryKey pk : pks1) {
            criteria1.addRow(pk);
        }
        request.addMultiRowQueryCriteria(criteria1);

        MultiRowQueryCriteria criteria2 = new MultiRowQueryCriteria("Table2");
        List<PrimaryKey> pks2 = new ArrayList<PrimaryKey>();
        pks2.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1)).build());
        pks2.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(2)).build());
        pks2.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(3)).build());
        pks2.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(4)).build());
        pks2.add(PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(5)).build());

        for (PrimaryKey pk : pks2) {
            criteria2.addRow(pk);
        }
        request.addMultiRowQueryCriteria(criteria2);

        // test no failed rows
        List<BatchGetRowResponse.RowResult> failedRows = new ArrayList<BatchGetRowResponse.RowResult>();
        BatchGetRowRequest retryRequest = request.createRequestForRetry(failedRows);
        assertTrue(retryRequest.isEmpty());

        // test multi failed rows with one table
        failedRows.add(new BatchGetRowResponse.RowResult("Table1", null, 1));
        failedRows.add(new BatchGetRowResponse.RowResult("Table1", null, 2));
        failedRows.add(new BatchGetRowResponse.RowResult("Table1", null, 4));

        retryRequest = request.createRequestForRetry(failedRows);
        assertTrue(!retryRequest.isEmpty());
        assertEquals(retryRequest.getCriteriasByTable().size(), 1);
        assertTrue(retryRequest.getCriteria("Table1") != null);
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().size(), 3);
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(0), pks1.get(1));
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(1), pks1.get(2));
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(2), pks1.get(4));

        // test multi failed rows with multi table
        failedRows.add(new BatchGetRowResponse.RowResult("Table2", null, 1));
        failedRows.add(new BatchGetRowResponse.RowResult("Table2", null, 3));

        retryRequest = request.createRequestForRetry(failedRows);
        assertTrue(!retryRequest.isEmpty());
        assertEquals(retryRequest.getCriteriasByTable().size(), 2);
        assertTrue(retryRequest.getCriteria("Table1") != null);
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().size(), 3);
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(0), pks1.get(1));
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(1), pks1.get(2));
        assertEquals(retryRequest.getCriteria("Table1").getRowKeys().get(2), pks1.get(4));
        assertTrue(retryRequest.getCriteria("Table2") != null);
        assertEquals(retryRequest.getCriteria("Table2").getRowKeys().size(), 2);
        assertEquals(retryRequest.getCriteria("Table2").getRowKeys().get(0), pks2.get(1));
        assertEquals(retryRequest.getCriteria("Table2").getRowKeys().get(1), pks2.get(3));

        // test failed rows with non-exist index
        List<BatchGetRowResponse.RowResult> tmp = new ArrayList<BatchGetRowResponse.RowResult>();
        tmp.addAll(failedRows);
        tmp.add(new BatchGetRowResponse.RowResult("Table2", null, 5));
        try {
            request.createRequestForRetry(tmp);
            fail();
        } catch (IllegalArgumentException e) {

        }

        // test failed rows with non-exist table
        tmp = new ArrayList<BatchGetRowResponse.RowResult>();
        tmp.addAll(failedRows);
        tmp.add(new BatchGetRowResponse.RowResult("Table3", null, 1));
        try {
            request.createRequestForRetry(tmp);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }
}
