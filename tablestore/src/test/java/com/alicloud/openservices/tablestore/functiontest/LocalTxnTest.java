package com.alicloud.openservices.tablestore.functiontest;


import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LocalTxnTest {
    String tableName = "test_local_txn";

    SyncClient client = null;

    @Before
    public void setUp() {
        ServiceSettings settings = ServiceSettings.load();

        client = new SyncClient(settings.getOTSEndpoint(), settings.getOTSAccessKeyId(),
                settings.getOTSAccessKeySecret(), settings.getOTSInstanceName());

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        Utils.deleteTableIfExist(client, tableName);

        Utils.waitForPartitionLoad(tableName);

        client.createTable(request);
    }

    @Test
    public void TestStartCommitTxn() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);

        String txnId = client.startLocalTransaction(request).getTransactionID();

        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        commitTxn(txnId);
    }

    @Test
    public void TestStartTxnTwice() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);

        String txnId = client.startLocalTransaction(request).getTransactionID();

        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        try {
            client.startLocalTransaction(request);
        } catch (TableStoreException ex) {
            assertEquals("OTSRowOperationConflict", ex.getErrorCode());
            if (!(ex.getMessage().equals("Data is being modified by the other request.") ||
                    ex.getMessage().equals("Failed to lock txn key: chengdu"))) {
                fail(ex.getErrorCode());
            }
        }

        commitTxn(txnId);

        try {
            commitTxn(txnId);
        } catch (TableStoreException ex) {
            assertEquals("OTSSessionNotExist", ex.getErrorCode());
            assertEquals("Session not exist or may be timeout.", ex.getMessage());
        }
    }

    @Test
    public void TestStartCommitTxnWithTwoPK() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(100));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);

        try {
            client.startLocalTransaction(request);
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("StartLocalTransaction support only one primary key.", ex.getMessage());
        }
    }

    @Test
    public void TestStartCommitTxnWithoutTableName() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest("", primaryKey);

        try {
            client.startLocalTransaction(request).getTransactionID();
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("Invalid table name: ''.", ex.getMessage());
        }
    }

    @Test
    public void TestCommitFailed() {
        try {
            CommitTransactionRequest commitRequest = new CommitTransactionRequest("");
            client.commitTransaction(commitRequest);
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("TransactionID is invalid.", ex.getMessage());
        }
    }

    @Test
    public void TestAbortFailed() {
        try {
            abortTxn("");
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("TransactionID is invalid.", ex.getMessage());
        }
    }

    @Test
    public void TestStartAbortTxn() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);

        String txnId = client.startLocalTransaction(request).getTransactionID();

        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        abortTxn(txnId);

        try {
            abortTxn(txnId);
        } catch (TableStoreException ex) {
            assertEquals("OTSSessionNotExist", ex.getErrorCode());
            assertEquals("Session not exist or may be timeout.", ex.getMessage());
        }
    }

    private void abortTxn(String txnId) {
        AbortTransactionRequest abortRequest = new AbortTransactionRequest(txnId);
        client.abortTransaction(abortRequest);
    }

    @Test
    public void TestStartPutCommitTxnGetRow() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        putRow(txnId, "chengdu", 101L, 1099L);

        commitTxn(txnId);

        Row row = getRow("", "chengdu", 101L);
        PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(101L, pks[ 1 ].getValue().asLong());

        Column[] columns = row.getColumns();
        assertEquals(1, columns.length);
        assertEquals("Col", columns[ 0 ].getName());
        assertEquals(1099, columns[ 0 ].getValue().asLong());
    }

    @Test
    public void TestStartBatchModifyCommitTxnGetRange() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        BatchModify(txnId, "chengdu", 102L, 1099L, 10);

        commitTxn(txnId);

        List<Row> rows = getRange("", "chengdu", 102L, 10005L);
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
            assertEquals(2, pks.length);
            assertEquals("pk1", pks[ 0 ].getName());
            assertEquals("chengdu", pks[ 0 ].getValue().asString());
            assertEquals("pk2", pks[ 1 ].getName());
            assertEquals(102L + i, pks[ 1 ].getValue().asLong());

            Column[] columns = row.getColumns();
            assertEquals(1, columns.length);
            assertEquals("Col", columns[ 0 ].getName());
            assertEquals(1099 + i, columns[ 0 ].getValue().asLong());
        }
    }

    @Test
    public void TestStartPutTwiceCommitTxnGetRow() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        putRow(txnId, "chengdu", 103L, 1099L);

        putRow(txnId, "chengdu", 104L, 2099L);

        commitTxn(txnId);

        Row row = getRow("", "chengdu", 103L);
        PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(103L, pks[ 1 ].getValue().asLong());

        Column[] columns = row.getColumns();
        assertEquals(1, columns.length);
        assertEquals("Col", columns[ 0 ].getName());
        assertEquals(1099, columns[ 0 ].getValue().asLong());

        row = getRow("", "chengdu", 104L);
        pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(104L, pks[ 1 ].getValue().asLong());

        columns = row.getColumns();
        assertEquals(1, columns.length);
        assertEquals("Col", columns[ 0 ].getName());
        assertEquals(2099, columns[ 0 ].getValue().asLong());
    }

    @Test
    public void TestStartPutTwiceAbortTxnGetRow() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        putRow(txnId, "chengdu", 105L, 1099L);

        putRow(txnId, "chengdu", 106L, 2099L);

        abortTxn(txnId);

        Row row = getRow("", "chengdu", 105L);
        assertTrue(row == null);

        row = getRow("", "chengdu", 106L);
        assertTrue(row == null);
    }

    @Test
    public void TestGetStartPutCommitTxn() {
        putRow("", "chengdu", 107L, 100L);

        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        Row row = getRow(txnId, "chengdu", 107L);
        PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(107L, pks[ 1 ].getValue().asLong());

        Column[] columns = row.getColumns();
        assertEquals(1, columns.length);
        assertEquals("Col", columns[ 0 ].getName());
        assertEquals(100L, columns[ 0 ].getValue().asLong());

        putRow(txnId, "chengdu", 108L, 1099L);

        commitTxn(txnId);
    }

    @Test
    public void TestStartPutGetCommitTxnGet() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        putRow(txnId, "chengdu", 109L, 1099L);

        Row row = getRow(txnId, "chengdu", 109L);
        PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(109L, pks[ 1 ].getValue().asLong());

        Column[] columns = row.getColumns();
        assertEquals(1, columns.length);
        assertEquals("Col", columns[ 0 ].getName());
        assertEquals(1099L, columns[ 0 ].getValue().asLong());
        assertTrue(columns[ 0 ].hasSetTimestamp());

        commitTxn(txnId);
    }

    @Test
    public void TestPutConfictTxn() {
        String txnId = startLocalTxn();
        assertNotNull(txnId);
        assertTrue(!txnId.isEmpty());

        putRow(txnId, "chengdu", 110L, 1099L);

        try {
            putRow("", "chengdu", 111L, 1099L);
        } catch (TableStoreException ex) {
            assertEquals("OTSRowOperationConflict", ex.getErrorCode());
            if (!(ex.getMessage().equals("Data is being modified by the other request.") ||
                    ex.getMessage().equals("Transaction timeout because cannot acquire exclusive lock."))) {
                fail(ex.getErrorCode());
            }
        }

        commitTxn(txnId);

        Row row = getRow("", "chengdu", 110L);
        PrimaryKeyColumn[] pks = row.getPrimaryKey().getPrimaryKeyColumns();
        assertEquals(2, pks.length);
        assertEquals("pk1", pks[ 0 ].getName());
        assertEquals("chengdu", pks[ 0 ].getValue().asString());
        assertEquals("pk2", pks[ 1 ].getName());
        assertEquals(110L, pks[ 1 ].getValue().asLong());
    }

    @Test
    public void TestCreateTableLocalTxnEnabled() {
        createTableWithLocalTxn(true);
        String transactionId = startLocalTxn();
        assertFalse(transactionId.isEmpty());
    }

    @Test
    public void TestCreateTableLocalTxnDisabled() {
        createTableWithLocalTxn(false);
        try {
            startLocalTxn();
            fail();
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("Try to call method using explicit transaction on explicit-transaction-disabled table.", ex.getMessage());
        }
    }

    @Test
    @Ignore("TestCreateTableLocalTxnEnabled and TestCreateTableLocalTxnDisabled are enough")
    public void TestCreateTableLocalTxnDefault() {
        createTableWithLocalTxn(null);
        try {
            startLocalTxn();
            fail();
        } catch (TableStoreException ex) {
            assertEquals("OTSParameterInvalid", ex.getErrorCode());
            assertEquals("Try to call method using explicit transaction on explicit-transaction-disabled table.", ex.getMessage());
        }
    }

    private String startLocalTxn() {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(tableName, primaryKey);

        return client.startLocalTransaction(request).getTransactionID();
    }


    private void putRow(String txnId, String pk1, Long pk2, Long columnValue) {
        {
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
            primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(pk2));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);

            // Add some property columns
            rowPutChange.addColumn(new Column("Col", ColumnValue.fromLong(columnValue)));

            PutRowRequest request = new PutRowRequest(rowPutChange);
            if (!txnId.isEmpty()) {
                request.setTransactionId(txnId);
            }
            client.putRow(request);
        }
    }

    private void putRow(String txnId, String pk1, Long pk2, Long columnValue, long timestamp) {
        {
            // Construct the primary key
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
            primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(pk2));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);

            // Add some property columns
            rowPutChange.addColumn(new Column("Col", ColumnValue.fromLong(columnValue), timestamp));

            PutRowRequest request = new PutRowRequest(rowPutChange);
            if (!txnId.isEmpty()) {
                request.setTransactionId(txnId);
            }
            client.putRow(request);
        }
    }

    private Row getRow(String txnId, String pk1, Long pk2) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder;
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
        primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(pk2));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        // Read one row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // Set to read the latest version
        criteria.setMaxVersions(1);
        GetRowRequest  request = new GetRowRequest(criteria);
        if (!txnId.isEmpty()) {
            request.setTransactionId(txnId);
        }
        GetRowResponse getRowResponse = client.getRow(request);
        return getRowResponse.getRow();
    }

    private List<Row> getRange(String txnId, String pk1, Long startPk2, Long endPk2) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);

        // Set the start primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
        primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(startPk2));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // Set the end primary key
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
        primaryKeyBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(endPk2));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
        return getRangeResponse.getRows();
    }

    private void BatchModify(String txnId, String pk1, Long pk2, Long columnValue, int count) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        // Construct rowPutChange
        for (int i = 0; i < count; i++) {
            PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pk1Builder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
            pk1Builder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(pk2 + i));
            RowPutChange rowPutChange1 = new RowPutChange(tableName, pk1Builder.build());
            // Add some columns
            rowPutChange1.addColumn(new Column("Col", ColumnValue.fromLong(columnValue + i)));

            // Add to batch operation
            batchWriteRowRequest.addRowChange(rowPutChange1);
        }

        // Construct rowUpdateChange
        for (int i = count; i < count + 3; i++) {
            PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pk1Builder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(pk1));
            pk1Builder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(pk2 + i));
            RowUpdateChange rowUpdateChange1 = new RowUpdateChange(tableName, pk1Builder.build());
            // Add some columns
            rowUpdateChange1.put(new Column("Col", ColumnValue.fromLong(columnValue + i)));

            // Add to batch operation
            batchWriteRowRequest.addRowChange(rowUpdateChange1);
        }

        if (!txnId.isEmpty()) {
            batchWriteRowRequest.setTransactionId(txnId);
        }

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);
        assertTrue(response.isAllSucceed());
    }

    private void commitTxn(String txnId) {
        CommitTransactionRequest commitRequest = new CommitTransactionRequest(txnId);
        client.commitTransaction(commitRequest);
    }

    private void createTableWithLocalTxn(Boolean enableLocalTxn) {
        // If the table already exists, delete the table.
        Utils.deleteTableIfExist(client, tableName);
        // Create table
        List<PrimaryKeySchema> pks = new ArrayList<PrimaryKeySchema>();
        pks.add(new PrimaryKeySchema("pk1", PrimaryKeyType.STRING));
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pks);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(100);
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        CreateTableRequest createTableRequest = new CreateTableRequest(meta, tableOptions);
        createTableRequest.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        if (enableLocalTxn != null) {
            createTableRequest.setLocalTxnEnabled(enableLocalTxn);
        }
        client.createTable(createTableRequest);
        // Wait for partition loading
        Utils.waitForPartitionLoad(tableName);
    }
}
