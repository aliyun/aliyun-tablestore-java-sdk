package com.aliyun.openservices.ots.integration;

import static com.aliyun.openservices.ots.OTSErrorCode.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.aliyun.openservices.ots.model.*;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.utils.ServiceSettings;


public class OTSClientTest {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;
    private static final int TABLE_OPERATION_INTERVAL_IN_MSEC = 1 * 1000;
    
    private String tableName = "ots_client_test_table";
    final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());

    private static final Logger LOG = Logger.getLogger(OTSClientTest.class.getName());

    @Before
    public void setup() throws Exception {
        LOG.info("Instance: " + ServiceSettings.load().getOTSInstanceName());

        ListTableResult r = ots.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);

            LOG.info("Delete table: " + table);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testTableOperation() throws Exception {
        LOG.info("Start testTableOperation");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(tableCU);
        ots.createTable(request);

        // list table to check table is exist
        List<String> tableNames = ots.listTable().getTableNames();
        assertTrue(tableNames.contains(tableName));

        // get table meta and check table meta
        DescribeTableRequest dtRequest = new DescribeTableRequest(tableName);
        DescribeTableResult dtResult = ots.describeTable(dtRequest);
        TableMeta meta = dtResult.getTableMeta();
        compareTableMeta(meta, tableMeta);
        assertTrue(dtResult.getReservedThroughputDetails().getLastIncreaseTime() != 0);
        assertEquals(dtResult.getReservedThroughputDetails().getLastDecreaseTime(), 0);
        assertEquals(dtResult.getReservedThroughputDetails().getNumberOfDecreasesToday(), 0);

        // update table
        // decrease read capacity
        Thread.sleep(70 * 1000 + 10); // sleep more than 10 minutes
        UpdateTableRequest utRequest = new UpdateTableRequest(tableName);
        ReservedThroughputChange capacityChange = new ReservedThroughputChange();
        capacityChange.setReadCapacityUnit(97);
        utRequest.setReservedThroughputChange(capacityChange);
        UpdateTableResult utResponse = ots.updateTable(utRequest);
        assertTrue(utResponse.getReservedThroughputDetails().getLastDecreaseTime() != 0);
        assertTrue(utResponse.getReservedThroughputDetails().getLastIncreaseTime() != 0);
        assertEquals(utResponse.getReservedThroughputDetails().getNumberOfDecreasesToday(), 1);
        assertEquals(utResponse.getReservedThroughputDetails().getCapacityUnit().getReadCapacityUnit(), 97);
        assertEquals(utResponse.getReservedThroughputDetails().getCapacityUnit().getWriteCapacityUnit(), 1);

        // get table meta and check table is updated
        dtResult = ots.describeTable(dtRequest);
        compareTableMeta(tableMeta,dtResult.getTableMeta());
        assertTrue(dtResult.getReservedThroughputDetails().getLastDecreaseTime() != 0);
        assertTrue(dtResult.getReservedThroughputDetails().getLastIncreaseTime() != 0);
        assertEquals(dtResult.getReservedThroughputDetails().getNumberOfDecreasesToday(), 1);
        assertEquals(dtResult.getReservedThroughputDetails().getCapacityUnit().getReadCapacityUnit(), 97);
        assertEquals(dtResult.getReservedThroughputDetails().getCapacityUnit().getWriteCapacityUnit(), 1);

        // decrease write capacity
        Thread.sleep(70 * 1000 + 10); // sleep more than 10 minutes
        capacityChange = new ReservedThroughputChange();
        capacityChange.setWriteCapacityUnit(98);
        utRequest.setReservedThroughputChange(capacityChange);
        utResponse = ots.updateTable(utRequest);
        assertTrue(utResponse.getReservedThroughputDetails().getLastDecreaseTime() != 0);
        assertTrue(utResponse.getReservedThroughputDetails().getLastIncreaseTime() != 0);
        assertEquals(utResponse.getReservedThroughputDetails().getNumberOfDecreasesToday(), 2);
        assertEquals(utResponse.getReservedThroughputDetails().getCapacityUnit().getReadCapacityUnit(), 97);
        assertEquals(utResponse.getReservedThroughputDetails().getCapacityUnit().getWriteCapacityUnit(), 98);

        // get table meta and check table is updated
        dtResult = ots.describeTable(dtRequest);
        compareTableMeta(tableMeta,dtResult.getTableMeta());
        assertTrue(dtResult.getReservedThroughputDetails().getLastDecreaseTime() != 0);
        assertTrue(dtResult.getReservedThroughputDetails().getLastIncreaseTime() != 0);
        assertEquals(dtResult.getReservedThroughputDetails().getNumberOfDecreasesToday(), 2);
        assertEquals(dtResult.getReservedThroughputDetails().getCapacityUnit().getReadCapacityUnit(), 97);
        assertEquals(dtResult.getReservedThroughputDetails().getCapacityUnit().getWriteCapacityUnit(), 98);

        // delete table
        DeleteTableRequest delRequest = new DeleteTableRequest(tableName);
        ots.deleteTable(delRequest);

        // list table to check table is not exist
        tableNames = ots.listTable().getTableNames();
        assertTrue(!tableNames.contains(tableName));
    }
    
    private void compareTableMeta(TableMeta meta, TableMeta tableMeta) {
        assertEquals(meta.getTableName(), tableMeta.getTableName());
        Map<String, PrimaryKeyType> pks1 = meta.getPrimaryKey();
        Map<String, PrimaryKeyType> pks2 = tableMeta.getPrimaryKey();
        assertEquals(pks1.size(), pks2.size());
        for (Entry<String, PrimaryKeyType> entry : pks1.entrySet()) {
            assertTrue(pks2.containsKey(entry.getKey()));
            assertEquals(pks2.get(entry.getKey()), entry.getValue());
        }
    }

    @Test
    public void testGetRowOperation() throws Exception {
        LOG.info("Start testGetRowOperation");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        ots.createTable(ctRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        // put some rows for test
        generateDataForTest(tableName, 1, 10);

        // get an exist row and all columns
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getRowPrimaryKeys(3));
        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        GetRowResult result = ots.getRow(request);
        compareRow(result.getRow(), 3, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});

        // get an exist row and specific columns (some columns are not exist)
        rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getRowPrimaryKeys(5));
        rowQueryCriteria.addColumnsToGet(new String[]{"name", "flag", "col_integer", "col_string", "non_exist_row", "col_boolean", "non_exist_row1", "non_exist_row2"});
        request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        result = ots.getRow(request);
        compareRow(result.getRow(), 5, new String[]{"name", "flag", "col_integer", "col_string", "col_boolean"});

        // get an non-exist row
        rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getRowPrimaryKeys(99));
        request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        result = ots.getRow(request);
        compareRow(result.getRow(), 99, new String[]{});

        // get row with invalid meta, expect exception
        rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        RowPrimaryKey pks = getRowPrimaryKeys(3);
        pks.addPrimaryKeyColumn("non_exist_pk", PrimaryKeyValue.fromLong(1));
        rowQueryCriteria.setPrimaryKey(pks);
        request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        try {
            result = ots.getRow(request);
            fail("Expect exception: get row with invalid meta.");
        } catch (OTSException e) {
        }
    }

    @Test
    public void testPutRowOperation() throws Exception {
        LOG.info("Start testPutRowOperation");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        ots.createTable(ctRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);

        /* put an row, get it and check all columns, also check consumed capacity unit */
        // put some rows for test
        generateDataForTest(tableName, 1, 10);

        // put an non-exist row with IGNORE
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 99;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_string", getColString(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            PutRowResult result = ots.putRow(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string"});
        }
        // put an non-exist row with EXPECT_EXIST, expect exception
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 98;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_string", getColString(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            try {
                ots.putRow(request);
                fail("Expect exception: put a non-exist row but expect exist.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), CONDITION_CHECK_FAIL);
            }
        }

        // put an non-exist row with EXPECT_NOT_EXIST
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 97;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_binary", getColBinary(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            PutRowResult result = ots.putRow(request);
            assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_binary"});
        }

        // put an exist row with IGNORE, with some new columns
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 3;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_boolean", getColBoolean(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_double", getColDouble(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            PutRowResult result = ots.putRow(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_boolean", "newrow_integer", "newrow_double"});
        }

        // put an exist row with EXPECT_EXIST, with some new columns
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 5;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_binary", getColBinary(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_boolean", getColBoolean(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            PutRowResult result = ots.putRow(request);
            assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_binary", "newrow_integer", "newrow_boolean"});
        }

        // put an exist row with EXPECT_NOT_EXIST, expect exception
        {
            RowPutChange rowChange = new RowPutChange(tableName);
            int i = 7;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_string", getColString(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            PutRowRequest request = new PutRowRequest();
            request.setRowChange(rowChange);
            try {
                ots.putRow(request);
                fail("Expect exception: put an exist row but expect not exist.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), CONDITION_CHECK_FAIL);
            }
        }
    }
    
    @Test
    public void testUpdateRowOperation() throws Exception {
        LOG.info("Start testUpdateRowOperation");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        ots.createTable(ctRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);

        // put some rows for test
        generateDataForTest(tableName, 1, 10);

        /* update an row, get it and check all columns, also check consumed capacity unit */
        // update an non-exist row with IGNORE
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 88;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            UpdateRowResult result = ots.updateRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(0, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "newrow_integer"});
        }

        // update an non-exist row with EXPECT_EXIST, expect exception
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 87;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            try {
                ots.updateRow(request);
                fail("Expect exception: update a non-exist row but expect exist.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), CONDITION_CHECK_FAIL);
            }
        }

        // update an non-exist row with EXPECT_NOT_EXIST, expect exception
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 86;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            try {
                ots.updateRow(request);
                fail("Expect exception: update row and expect row not exist.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), "OTSParameterInvalid");
            }
        }

        // update an exist row with IGNORE, add some new columns and delete some columns
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 3;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("col_string");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            UpdateRowResult result = ots.updateRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "newrow_integer", "col_double", "col_boolean", "col_binary"});
        }

        // update an exist-row, modify some old columns
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 88;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", ColumnValue.fromString("hello world"));
            rowChange.addAttributeColumn("col_boolean", ColumnValue.fromString("hello world"));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            UpdateRowResult result = ots.updateRow(request);

            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);

            CheckSpecificColumn(tableName, i, "col_boolean", ColumnValue.fromString("hello world"));
        }

        // update an exist row with EXPECT_EXIST
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 5;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("col_string");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            UpdateRowResult result = ots.updateRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{"uid", "name", "sid", "flag", "newrow_integer", "col_double", "col_boolean", "col_binary"});
        }

        // update an exist row with EXPECT_NOT_EXIST, expect exception
        {
            RowUpdateChange rowChange = new RowUpdateChange(tableName);
            int i = 1;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("newrow_integer", getColInteger(i));
            rowChange.deleteAttributeColumn("col_integer");
            rowChange.deleteAttributeColumn("newrow_string");

            UpdateRowRequest request = new UpdateRowRequest();
            request.setRowChange(rowChange);
            try {
                ots.updateRow(request);
                fail("Expect exception: update row and expect row not exist.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), "OTSParameterInvalid");
            }
        }
    }
    
    @Test
    public void testDeleteRowOperation() throws Exception {
        LOG.info("Start testDeleteRowOperation");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        ots.createTable(ctRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);

        // put some rows for test
        generateDataForTest(tableName, 1, 10);

        /* delete an row, get it and check if the row is deleted, also check consumed capacity unit */
        // delete an non-exist row with IGNORE
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 99;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            DeleteRowResult result = ots.deleteRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{});
        }

        // delete an non-exist row with EXPECT_EXIST, expect exception
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 88;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            try {
                ots.deleteRow(request);
                fail("Expect exception: delete a non-exist row and expect it exists.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), CONDITION_CHECK_FAIL);
            }
        }

        // delete an non-exist row with EXPECT_NOT_EXIST, expect exception
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 77;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            try {
                ots.deleteRow(request);
                fail("Expect exception: delete a non-exist row and expect it not exists.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), "OTSParameterInvalid");
            }
        }

        // delete an exist row with IGNORE
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 3;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            DeleteRowResult result = ots.deleteRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{});
        }

        // delete an exist row with EXPECT_EXIST
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 5;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            DeleteRowResult result = ots.deleteRow(request);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
            assertEquals(1, result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());

            Row row = readRow(tableName, i);
            compareRow(row, i, new String[]{});
        }

        // delete an exist row with EXPECT_NOT_EXIST, expect exception
        {
            RowDeleteChange rowChange = new RowDeleteChange(tableName);
            int i = 7;
            rowChange.setPrimaryKey(getRowPrimaryKeys(i));
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
            rowChange.setCondition(condition);

            DeleteRowRequest request = new DeleteRowRequest();
            request.setRowChange(rowChange);

            try {
                ots.deleteRow(request);
                fail("Expect exception: delete a non-exist row and expect it not exists.");
            } catch (OTSException e) {
                assertEquals(e.getErrorCode(), "OTSParameterInvalid");
            }
        }
    }
    
    @Test
    public void testGetRange() throws Exception {
        LOG.info("Start testGetRange");

        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        ots.createTable(ctRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);

        // put some rows for test
        generateDataForTest(tableName, 0, 10000);

        // test get range each time with one primary key value range from INF_MIN to INF_MAX and other primary key in a specific value
        {
            // test flag from INF_MIN to INF_MAX
            int i = 3;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("flag", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("flag", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertEquals(result.getNextStartPrimaryKey(), null);
            assertEquals(result.getRows().size(), 1);
            compareRow(result.getRows().get(0), i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
        }
        {
            // test name from INF_MIN to INF_MAX
            int i = 3;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertEquals(result.getNextStartPrimaryKey(), null);
            assertEquals(result.getRows().size(), 1);
            compareRow(result.getRows().get(0), i, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
        }
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertTrue(result.getNextStartPrimaryKey() != null);
            assertEquals(result.getRows().size(), result.getNextStartPrimaryKey().getPrimaryKey().get("uid").asLong());
            int index = 0;
            for (Row row : result.getRows()) {
                compareRow(row, index, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
                index++;
            }
        }

        // test get range in primary keys all in different value
        {
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            // [0,'00001',2 -> 100,'00099',0), total 99 rows
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(0));
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("00001"));
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromLong(2));

            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(100));
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("00099"));
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromLong(0));

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            criteria.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary"});

            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertTrue(result.getNextStartPrimaryKey() == null);
            assertEquals(result.getRows().size(), 99);
            int index = 1;
            for (Row row : result.getRows()) {
                compareRow(row, index, new String[]{"col_integer", "col_string",  "col_binary"});
                index++;
            }
        }

        // test get range with limit
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int limit = 999;
            criteria.setLimit(limit);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertTrue(result.getNextStartPrimaryKey() != null);
            assertEquals(result.getRows().size(), limit);
            int index = 0;
            for (Row row : result.getRows()) {
                compareRow(row, index, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
                index++;
            }
        }

        // test get a large range, expect next start key is returned, and with next start key, we can read all rows in the range
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
            criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int limit = 999;
            criteria.setLimit(limit);
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(criteria);

            GetRangeResult result = ots.getRange(request);
            assertEquals(result.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
            assertTrue(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);

            assertTrue(result.getNextStartPrimaryKey() != null);
            assertEquals(result.getRows().size(), limit);
            int index = 0;
            for (Row row : result.getRows()) {
                compareRow(row, index, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
                index++;
            }
        }

        // test get a large range with iterator, expect all rows in range is returned
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), 10000);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_boolean", "col_double", "col_binary"});
                index++;
            }
        }

        // test get a large range with iterator, set limit, expect all rows in range is returned
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int count = 4000;
            parameter.setCount(count);
            parameter.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary", "uid"});

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), count);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "col_integer", "col_string", "col_binary"});
                index++;
            }
        }
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int count = 4000;
            parameter.setCount(count);
            parameter.setBufferSize(1000);
            parameter.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary", "uid"});

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), count);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "col_integer", "col_string", "col_binary"});
                index++;
            }
        }
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            parameter.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary", "uid"});

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), 10000);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "col_integer", "col_string", "col_binary"});
                index++;
            }
        }
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            parameter.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary", "uid"});
            parameter.setBufferSize(1000);

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), 10000);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "col_integer", "col_string", "col_binary"});
                index++;
            }
        }
        {
            // test uid from INF_MIN to INF_MAX
            int i = 0;
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(i);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(i);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int count = 4000;
            parameter.setCount(count);
            parameter.setBufferSize(5000);
            parameter.addColumnsToGet(new String[]{"col_integer", "col_string", "col_binary", "uid"});

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), count);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "col_integer", "col_string", "col_binary"});
                index++;
            }
        }

        // test a really large data set
        {
            generateDataForTest(tableName, 1000, 59999);
            // test uid from 1000 to 100001
            RowPrimaryKey inclusiveStartPrimaryKey = getRowPrimaryKeys(0);
            RowPrimaryKey exclusiveEndPrimaryKey = getRowPrimaryKeys(0);
            inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(0));
            exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(100001));

            RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
            parameter.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
            parameter.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
            int count = 34567;
            parameter.setCount(count);
            parameter.addColumnsToGet(new String[]{"uid", "name", "col_boolean", "col_double", "col_binary"});

            List<Row> allRows = new ArrayList<Row>();
            Iterator<Row> iter = ots.createRangeIterator(parameter);
            while (iter.hasNext()) {
                allRows.add(iter.next());
            }
            assertEquals(allRows.size(), count);

            int index = 0;
            for (Row row : allRows) {
                compareRow(row, index, new String[]{"uid", "name", "col_boolean", "col_double", "col_binary"});
                index++;
            }
        }
    }
    
    @Test
    public void testBatchGetRow() throws Exception {
        LOG.info("Start testBatchGetRow");

        final int TABLE_COUNT = 10;
        final int ROW_COUNT = 100;
        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        String[] tableNames = new String[TABLE_COUNT];
        for (int i = 0; i < TABLE_COUNT; i++) {
            tableNames[i] = tableName + "_" + i;
            tableMeta.setTableName(tableNames[i]);
            ctRequest.setTableMeta(tableMeta);
            ots.createTable(ctRequest);

            Thread.sleep(TABLE_OPERATION_INTERVAL_IN_MSEC);
        }
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        for (int i = 0; i < TABLE_COUNT; i++) {
            generateDataForTest(tableNames[i], i * ROW_COUNT, i * ROW_COUNT + ROW_COUNT);
        }

        // test batch get rows, expect all rows is returned, and all rows is in the right index
        BatchGetRowRequest request = new BatchGetRowRequest();
        int rowsGet = 1;
        for (int i = 0; i < TABLE_COUNT; i++) {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableNames[i]);
            for (int j = 0; j < rowsGet; j++) {
                criteria.addRow(getRowPrimaryKeys(i * ROW_COUNT + j));
                request.addMultiRowQueryCriteria(criteria);
            }
            criteria.addColumnsToGet(new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_binary"});
        }

        BatchGetRowResult result = ots.batchGetRow(request);
        for (int i = 0; i < TABLE_COUNT; i++) {
            List<BatchGetRowResult.RowStatus> status = result.getBatchGetRowStatus(tableNames[i]);
            assertEquals(status.size(), rowsGet);
            for (int j = 0; j < rowsGet; j++) {
                assertTrue(status.get(j).isSucceed());
                assertTrue(status.get(j).getConsumedCapacity().getCapacityUnit().getReadCapacityUnit() != 0);
                assertEquals(status.get(j).getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit(), 0);
                Row row = status.get(j).getRow();
                compareRow(row, i * ROW_COUNT + j, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string", "col_binary"});
            }
        }
    }
    
    @Test
    public void testBatchWriteRow() throws Exception {
        LOG.info("Start testBatchWriteRow");

        final int TABLE_COUNT = 10;
        final int ROW_COUNT = 100;
        
        // create table
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestCapacityUnit();
        CreateTableRequest ctRequest = new CreateTableRequest();
        ctRequest.setTableMeta(tableMeta);
        ctRequest.setReservedThroughput(tableCU);
        String[] tableNames = new String[TABLE_COUNT];
        for (int i = 0; i < TABLE_COUNT; i++) {
            tableNames[i] = tableName + "_" + i;
            tableMeta.setTableName(tableNames[i]);
            ctRequest.setTableMeta(tableMeta);
            ots.createTable(ctRequest);

            Thread.sleep(TABLE_OPERATION_INTERVAL_IN_MSEC);
        }
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        for (int i = 0; i < TABLE_COUNT; i++) {
            generateDataForTest(tableNames[i], 0, ROW_COUNT);
        }

        // test batch write rows, only with put, and some rows will with condition check fail, check result is as expected and rows in response is in the right index
        {
            // test batch get rows, expect all rows is returned, and all rows is in the right index
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            final int rowsGet = 10;
            final int startIndex = 10000;
            for (int i = 0; i < TABLE_COUNT; i++) {
                for (int j = 0; j < rowsGet; j++) {
                    int id = j + startIndex;
                    RowPutChange rowChange = new RowPutChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(id));
                    rowChange.addAttributeColumn("col_integer", getColInteger(id));
                    rowChange.addAttributeColumn("col_string", getColString(id));
                    Condition condition = new Condition();
                    if (j % 2 == 0) {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    } else {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST); // will condition check fail
                    }
                    rowChange.setCondition(condition);
                    request.addRowPutChange(rowChange);
                }
            }

            BatchWriteRowResult result = ots.batchWriteRow(request);
            for (int i = 0; i < TABLE_COUNT; i++) {
                List<BatchWriteRowResult.RowStatus> status = result.getPutRowStatus(tableNames[i]);
                assertEquals(status.size(), rowsGet);
                for (int j = 0; j < rowsGet; j++) {
                    if (j % 2 == 0) {
                        assertTrue(status.get(j).isSucceed());
                        assertTrue(status.get(j).getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
                        assertEquals(status.get(j).getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);
                        assertEquals(status.get(j).getError(), null);

                        Row row = readRow(tableNames[i], j + startIndex);
                        compareRow(row, j + startIndex, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string"});
                    } else {
                        assertTrue(!status.get(j).isSucceed());
                        assertEquals(status.get(j).getConsumedCapacity(), null);
                        assertEquals(status.get(j).getError().getCode(), CONDITION_CHECK_FAIL);
                    }
                }
            }
        }

        // test batch write rows, only with update, and some rows will with condition check fail, check result is as expected and rows in response is in the right index
        {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            final int rowsGet = 10;
            for (int i = 0; i < TABLE_COUNT; i++) {
                for (int j = 0; j < rowsGet; j++) {
                    int id = j % 2 == 0 ? j : j + 10000; // row exist when j % 2 == 0
                    RowUpdateChange rowChange = new RowUpdateChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(id));
                    rowChange.addAttributeColumn("col_integer", getColInteger(id));
                    rowChange.addAttributeColumn("col_string", getColString(id));
                    rowChange.deleteAttributeColumn("col_double");
                    rowChange.deleteAttributeColumn("col_boolean");
                    rowChange.deleteAttributeColumn("col_binary");
                    Condition condition = new Condition();
                    if (j % 2 == 0) {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    } else {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST); // will condition check fail
                    }
                    rowChange.setCondition(condition);
                    request.addRowUpdateChange(rowChange);
                }
            }

            BatchWriteRowResult result = ots.batchWriteRow(request);
            for (int i = 0; i < TABLE_COUNT; i++) {
                List<BatchWriteRowResult.RowStatus> status = result.getUpdateRowStatus(tableNames[i]);
                assertEquals(status.size(), rowsGet);
                for (int j = 0; j < rowsGet; j++) {
                    if (j % 2 == 0) {
                        assertTrue(status.get(j).isSucceed());
                        assertTrue(status.get(j).getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
                        assertEquals(status.get(j).getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);
                        assertEquals(status.get(j).getError(), null);

                        Row row = readRow(tableNames[i], j);
                        compareRow(row, j, new String[]{"uid", "name", "sid", "flag", "col_integer", "col_string"});
                    } else {
                        assertTrue(!status.get(j).isSucceed());
                        assertEquals(status.get(j).getConsumedCapacity(), null);
                        assertEquals(status.get(j).getError().getCode(), CONDITION_CHECK_FAIL);
                    }
                }
            }
        }

        // test batch write rows, only with delete, and some rows will with condition check fail, check result is as expected and rows in response is in the right index
        {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            final int rowsGet = 10;
            for (int i = 0; i < TABLE_COUNT; i++) {
                for (int j = 0; j < rowsGet; j++) {
                    int id = j % 2 == 0 ? j : j + 10000; // row exist when j % 2 == 0
                    RowDeleteChange rowChange = new RowDeleteChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(id));
                    Condition condition = new Condition();
                    if (j % 2 == 0) {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    } else {
                        condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST); // will condition check fail
                    }
                    rowChange.setCondition(condition);
                    request.addRowDeleteChange(rowChange);
                }
            }

            BatchWriteRowResult result = ots.batchWriteRow(request);
            for (int i = 0; i < TABLE_COUNT; i++) {
                List<BatchWriteRowResult.RowStatus> status = result.getDeleteRowStatus(tableNames[i]);
                assertEquals(status.size(), rowsGet);
                for (int j = 0; j < rowsGet; j++) {
                    if (j % 2 == 0) {
                        assertTrue(status.get(j).isSucceed());
                        assertTrue(status.get(j).getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit() != 0);
                        assertEquals(status.get(j).getConsumedCapacity().getCapacityUnit().getReadCapacityUnit(), 0);
                        assertEquals(status.get(j).getError(), null);

                        Row row = readRow(tableNames[i], j);
                        compareRow(row, j, new String[]{});
                    } else {
                        assertTrue(!status.get(j).isSucceed());
                        assertEquals(status.get(j).getConsumedCapacity(), null);
                        assertEquals(status.get(j).getError().getCode(), CONDITION_CHECK_FAIL);
                    }
                }
            }
        }

        // test batch write rows with various operation, and some rows will with condition check fail, check result is as expected and rows in response is in the right index
        {
            BatchWriteRowRequest request = new BatchWriteRowRequest();
            for (int i = 0; i < TABLE_COUNT; i++) {
                {
                    // put an non-exist row
                    RowPutChange rowChange = new RowPutChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(777));
                    rowChange.addAttributeColumn("col_integer", getColInteger(777));
                    rowChange.addAttributeColumn("col_string", getColString(777));
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    rowChange.setCondition(condition);
                    request.addRowPutChange(rowChange);
                }
                {
                    // put an exist row and expect not exist, will fail
                    RowPutChange rowChange = new RowPutChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(3));
                    rowChange.addAttributeColumn("col_integer", getColInteger(3));
                    rowChange.addAttributeColumn("col_string", getColString(3));
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST);
                    rowChange.setCondition(condition);
                    request.addRowPutChange(rowChange);
                }
                {
                    // update an exist row
                    RowUpdateChange rowChange = new RowUpdateChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(7));
                    rowChange.addAttributeColumn("col_integer", getColInteger(7));
                    rowChange.addAttributeColumn("col_string", getColString(7));
                    rowChange.deleteAttributeColumn("col_double");
                    rowChange.deleteAttributeColumn("col_boolean");
                    rowChange.deleteAttributeColumn("col_binary");
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    rowChange.setCondition(condition);
                    request.addRowUpdateChange(rowChange);
                }
                {
                    // update an non-exist row and expect exist
                    RowUpdateChange rowChange = new RowUpdateChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(999));
                    rowChange.addAttributeColumn("col_integer", getColInteger(7));
                    rowChange.addAttributeColumn("col_string", getColString(7));
                    rowChange.deleteAttributeColumn("col_double");
                    rowChange.deleteAttributeColumn("col_boolean");
                    rowChange.deleteAttributeColumn("col_binary");
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
                    rowChange.setCondition(condition);
                    request.addRowUpdateChange(rowChange);
                }
                {
                    // delete an exist row
                    RowDeleteChange rowChange = new RowDeleteChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(0));
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.IGNORE);
                    rowChange.setCondition(condition);
                    request.addRowDeleteChange(rowChange);
                }
                {
                    // delete an non-exist row and expect exist
                    RowDeleteChange rowChange = new RowDeleteChange(tableNames[i]);
                    rowChange.setPrimaryKey(getRowPrimaryKeys(7777));
                    Condition condition = new Condition();
                    condition.setRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST);
                    rowChange.setCondition(condition);
                    request.addRowDeleteChange(rowChange);
                }
            }

            BatchWriteRowResult result = ots.batchWriteRow(request);
            for (int i = 0; i < TABLE_COUNT; i++) {
                {
                    List<BatchWriteRowResult.RowStatus> status = result.getPutRowStatus(tableNames[i]);
                    assertEquals(status.size(), 2);
                    assertTrue(status.get(0).isSucceed());
                    Row row = readRow(tableNames[i], 777);
                    compareRow(row, 777, new String[]{"col_integer", "col_string", "uid", "name", "sid", "flag"});

                    assertTrue(!status.get(1).isSucceed());
                    assertEquals(status.get(1).getError().getCode(), CONDITION_CHECK_FAIL);
                }
                {
                    List<BatchWriteRowResult.RowStatus> status = result.getUpdateRowStatus(tableNames[i]);
                    assertEquals(status.size(), 2);
                    assertTrue(status.get(0).isSucceed());
                    Row row = readRow(tableNames[i], 7);
                    compareRow(row, 7, new String[]{"col_integer", "col_string", "uid", "name", "sid", "flag"});

                    assertTrue(!status.get(1).isSucceed());
                    assertEquals(status.get(1).getError().getCode(), CONDITION_CHECK_FAIL);
                }
                {
                    List<BatchWriteRowResult.RowStatus> status = result.getDeleteRowStatus(tableNames[i]);
                    assertEquals(status.size(), 2);
                    assertTrue(status.get(0).isSucceed());
                    Row row = readRow(tableNames[i], 0);
                    compareRow(row, 0, new String[]{});
                }
            }
        }
    }

    @Test
    public void testCreateTableWithBinaryKey() {
        LOG.info("Start testCreateTableWithBinaryKey");

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.BINARY);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BINARY);

        CapacityUnit cu = new CapacityUnit(100, 100);
        CreateTableRequest ctr = new CreateTableRequest();
        ctr.setTableMeta(tableMeta);
        ctr.setReservedThroughput(cu);

        ots.createTable(ctr);
    }

    private TableMeta getTestTableMeta() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("uid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("name", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("sid", PrimaryKeyType.BINARY);
        tableMeta.addPrimaryKeyColumn("flag", PrimaryKeyType.INTEGER);
        return tableMeta;
    }
    
    private CapacityUnit getTestCapacityUnit() {
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(1);
        capacityUnit.setWriteCapacityUnit(1);
        return capacityUnit;
    }
    
    private Row readRow(String tableName, int i) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(tableName);
        rowQueryCriteria.setPrimaryKey(getRowPrimaryKeys(i));
        GetRowRequest request = new GetRowRequest();
        request.setRowQueryCriteria(rowQueryCriteria);
        GetRowResult result = ots.getRow(request);
        return result.getRow();
    }
    
    private void CheckSpecificColumn(String tableName, int i, String columnName, ColumnValue columnValue) {
        Row row = readRow(tableName, i);
        assertTrue(row.getColumns().containsKey(columnName));
        assertEquals(row.getColumns().get(columnName), columnValue);
    }
    
    private ColumnValue getColInteger(int i) {
        return ColumnValue.fromLong(i);
    }
    
    private ColumnValue getColString(int i) {
        return ColumnValue.fromString(String.format("%05d", i));
    }
    
    private ColumnValue getColDouble(int i) {
        return ColumnValue.fromDouble(999.99);
    }
    
    private ColumnValue getColBoolean(int i) {
        return ColumnValue.fromBoolean(i % 2 == 0);
    }
    
    private ColumnValue getColBinary(int i) {
        return ColumnValue.fromBinary(new byte[]{(byte)(i % 127), (byte)(i % 127), (byte)(i % 127)});
    }
    
    /**
     * 每一行都包含固定的几个列: uid, name, flag, col_integer, col_string, col_double, col_boolean, col_binary。
     * 也可以添加自定义的新列，但是需要遵循一定的规则, integer类型的列必须以"integer"结尾，其他类型都遵循此规则。
     * 同一类型的列取值都固定，需要调用想用的getColXXX函数去获取值。
     * @param row
     * @param i
     * @param cs
     */
    private void compareRow(Row row, int i, String[] cs) {
        Set<String> tmp = new HashSet<String>();
        for (String s : cs) {
            tmp.add(s);
        }
        String[] columns = tmp.toArray(new String[0]);
        assertEquals(row.getColumns().size(), columns.length);
        for (String column : columns) {
            assertTrue(row.getColumns().get(column) != null);
            if (column.equals("uid")) {
                assertEquals(row.getColumns().get(column), ColumnValue.fromLong(i));
            } else if (column.equals("name")) {
                assertEquals(row.getColumns().get(column), ColumnValue.fromString(String.format("%05d", i)));
            } else if (column.equals("flag")) {
                assertEquals(row.getColumns().get(column), ColumnValue.fromLong(i * i));
            } else if (column.equals("sid")) {
                assertEquals(row.getColumns().get(column), ColumnValue.fromBinary(makeBinaryValue(i)));
            } else if (column.equals("col_integer")) {
                assertEquals(row.getColumns().get(column), getColInteger(i));
            } else if (column.equals("col_double")) {
                assertEquals(row.getColumns().get(column), getColDouble(i));
            } else if (column.equals("col_boolean")) {
                assertEquals(row.getColumns().get(column), getColBoolean(i));
            } else if (column.equals("col_string")) {
                assertEquals(row.getColumns().get(column), getColString(i));
            } else if (column.equals("col_binary")) {
                assertArrayEquals(row.getColumns().get(column).asBinary(), getColBinary(i).asBinary());
            } else if (column.endsWith("string")) {
                assertEquals(row.getColumns().get(column), getColString(i));
            } else if (column.endsWith("integer")) {
                assertEquals(row.getColumns().get(column), getColInteger(i));
            } else if (column.endsWith("boolean")) {
                assertEquals(row.getColumns().get(column), getColBoolean(i));
            } else if (column.endsWith("double")) {
                assertEquals(row.getColumns().get(column), getColDouble(i));
            } else if (column.endsWith("binary")) {
                assertEquals(row.getColumns().get(column), getColBinary(i));
            }
        }
    }
    
    private RowPrimaryKey getRowPrimaryKeys(int i) {
        RowPrimaryKey primaryKeys = new RowPrimaryKey();
        primaryKeys.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(i));
        primaryKeys.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString(String.format("%05d", i)));
        primaryKeys.addPrimaryKeyColumn("sid", PrimaryKeyValue.fromBinary(makeBinaryValue(i)));
        primaryKeys.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromLong(i * i));
        return primaryKeys;
    }

    private byte[] makeBinaryValue(int i) {
        byte[] s = new byte[13];
        for (int j = 0; j < s.length; j++) {
            s[j] = (byte)(i+j);
        }
        return s;
    }

    private void writeRows(BatchWriteRowRequest request) {
        while (true) {
            try {
                ots.batchWriteRow(request);
                break;
            } catch (OTSException e) {
                // just ignore
            }
        }
    }
    
    private void generateDataForTest(String tableName, int from, int to) {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        final int ROWS_PER_REQUEST = 10;
        for (int i = from, count = 1; i < to; i++, count++) {

            RowPutChange rowChange = new RowPutChange(tableName);
            RowPrimaryKey primaryKeys = getRowPrimaryKeys(i);
            
            rowChange.setPrimaryKey(primaryKeys);
            rowChange.addAttributeColumn("col_integer", getColInteger(i));
            rowChange.addAttributeColumn("col_string", getColString(i));
            rowChange.addAttributeColumn("col_double", getColDouble(i));
            rowChange.addAttributeColumn("col_boolean", getColBoolean(i));
            rowChange.addAttributeColumn("col_binary", getColBinary(i));
            
            request.addRowPutChange(rowChange);
            if (count % ROWS_PER_REQUEST == 0) {
                writeRows(request);
                request = new BatchWriteRowRequest();
            }
        }
        
        if (!request.getRowPutChange().isEmpty()) {
            writeRows(request);
        }
    }
}
