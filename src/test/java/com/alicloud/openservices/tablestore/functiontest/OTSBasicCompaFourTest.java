package com.alicloud.openservices.tablestore.functiontest;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.OTSRestrictedItemConst;
import com.alicloud.openservices.tablestore.common.Utils;
import com.google.gson.JsonSyntaxException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Created by jingwen.hjw on 2015/1/31.
 */
public class OTSBasicCompaFourTest extends BaseFT {

    private static String tableName = "OTSBasicCompaTest";
    private static SyncClientInterface ots;
    private static final Logger LOG = LoggerFactory.getLogger(OTSBasicCompaFourTest.class);

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ots = Utils.getOTSInstance();
    }

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Before
    public void setup() throws Exception {
        // 清理环境
        OTSHelper.deleteAllTable(ots);
    }

    @After
    public void teardown() throws Exception {
    }

    private void createTableWithOnePrimaryKey(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);

        Utils.waitForPartitionLoad(tableName);
        DescribeTableResponse describeTableResult = ots.describeTable(new DescribeTableRequest(tableName));
        assertEquals(capacityUnit, describeTableResult.getReservedThroughputDetails().getCapacityUnit());

        assertEquals(tableMeta.getTableName(), describeTableResult.getTableMeta().getTableName());
        assertEquals(tableMeta.getPrimaryKeyList().size(), describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals(1, describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals("PK1", describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals(PrimaryKeyType.STRING, describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getType());

        assertEquals(1, describeTableResult.getTableOptions().getMaxVersions());
        assertEquals(Integer.MAX_VALUE, describeTableResult.getTableOptions().getTimeToLive());
    }

    private void createTableWithTwoPrimaryKeys(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
        DescribeTableResponse describeTableResult = ots.describeTable(new DescribeTableRequest(tableName));
        assertEquals(capacityUnit, describeTableResult.getReservedThroughputDetails().getCapacityUnit());

        assertEquals(tableMeta.getTableName(), describeTableResult.getTableMeta().getTableName());
        assertEquals(tableMeta.getPrimaryKeyList().size(), describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals(2, describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals("PK1", describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals(PrimaryKeyType.STRING, describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getType());
        assertEquals("PK2", describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getType());

        assertEquals(1, describeTableResult.getTableOptions().getMaxVersions());
        assertEquals(Integer.MAX_VALUE, describeTableResult.getTableOptions().getTimeToLive());
    }

    private void createTableWithThreePrimaryKeys(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        pk.add(new PrimaryKeySchema("PK3", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);

        DescribeTableResponse describeTableResult = ots.describeTable(new DescribeTableRequest(tableName));
        assertEquals(capacityUnit, describeTableResult.getReservedThroughputDetails().getCapacityUnit());

        assertEquals(tableMeta.getTableName(), describeTableResult.getTableMeta().getTableName());
        assertEquals(tableMeta.getPrimaryKeyList().size(), describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals(3, describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals("PK1", describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals(PrimaryKeyType.STRING, describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getType());
        assertEquals("PK2", describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getType());
        assertEquals("PK3", describeTableResult.getTableMeta().getPrimaryKeyList().get(2).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(2).getType());

        assertEquals(1, describeTableResult.getTableOptions().getMaxVersions());
        assertEquals(Integer.MAX_VALUE, describeTableResult.getTableOptions().getTimeToLive());
    }

    private void createTableWithFourPrimaryKeys(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        pk.add(new PrimaryKeySchema("PK3", PrimaryKeyType.INTEGER));
        pk.add(new PrimaryKeySchema("PK4", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);

        DescribeTableResponse describeTableResult = ots.describeTable(new DescribeTableRequest(tableName));
        assertEquals(capacityUnit, describeTableResult.getReservedThroughputDetails().getCapacityUnit());

        assertEquals(tableMeta.getTableName(), describeTableResult.getTableMeta().getTableName());
        assertEquals(tableMeta.getPrimaryKeyList().size(), describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals(4, describeTableResult.getTableMeta().getPrimaryKeyList().size());
        assertEquals("PK1", describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals(PrimaryKeyType.STRING, describeTableResult.getTableMeta().getPrimaryKeyList().get(0).getType());
        assertEquals("PK2", describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(1).getType());
        assertEquals("PK3", describeTableResult.getTableMeta().getPrimaryKeyList().get(2).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(2).getType());
        assertEquals("PK4", describeTableResult.getTableMeta().getPrimaryKeyList().get(3).getName());
        assertEquals(PrimaryKeyType.INTEGER, describeTableResult.getTableMeta().getPrimaryKeyList().get(3).getType());

        assertEquals(1, describeTableResult.getTableOptions().getMaxVersions());
        assertEquals(Integer.MAX_VALUE, describeTableResult.getTableOptions().getTimeToLive());
    }

    private void createTableWithOnePrimaryKeyString(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
    }

    private void createTableWithOnePrimaryKeyInteger(String tableName) throws Exception {

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
    }

    private void getRangeForMultiRangeGet(String tableName,
                                          PrimaryKeyValue startFirst,
                                          PrimaryKeyValue startSecond,
                                          PrimaryKeyValue endFirst,
                                          PrimaryKeyValue endSecond)  throws Exception {
        // PrimaryKey
        List<PrimaryKeyColumn> primaryKeyList;

        // GetRange
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", startFirst));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", startSecond));
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", endFirst));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", endSecond));
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        GetRangeResponse getRangeResult = ots.getRange(getRangeRequest);
        //assertEquals(new CapacityUnit(0, 0), getRangeResult.getConsumedCapacity().getCapacityUnit());
        assertEquals(0, getRangeResult.getRows().size());
    }

    private void createTableWithPrimaryKeyStringString(String tableName) {
        // create Table
        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.STRING));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);

        Utils.waitForPartitionLoad(tableName);
    }

    private void createTableWithPrimaryKeyStringInteger(String tableName) {
        // create Table
        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
    }

    private void createTableWithPrimaryKeyIntegerInteger(String tableName) {
        // create Table
        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
    }

    private void createTableWithPrimaryKeyIntegerString(String tableName) {
        // create Table
        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.INTEGER));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.STRING));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);
    }

    private void validateGetRangeWithoutParition(String tableName,
                                                 PrimaryKey start, PrimaryKey end, int expectation) {
        byte[] binaryValue = new byte[1];
        binaryValue[0] = (byte) 0xff;

        // GetRange
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.addColumnsToGet("C2");
        rangeRowQueryCriteria.addColumnsToGet("C3");
        rangeRowQueryCriteria.addColumnsToGet("C4");
        rangeRowQueryCriteria.addColumnsToGet("C5");
        rangeRowQueryCriteria.addColumnsToGet("C6");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(start);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        GetRangeResponse getRangeResult = ots.getRange(getRangeRequest);
        assertEquals(expectation, getRangeResult.getRows().size());

        for (Row row : getRangeResult.getRows()) {
            assertEquals(1, row.getColumn("C1").size());
            assertEquals(1, row.getColumn("C2").size());
            assertEquals(1, row.getColumn("C3").size());
            assertEquals(1, row.getColumn("C4").size());
            assertEquals(1, row.getColumn("C5").size());
            assertEquals(1, row.getColumn("C6").size());

            assertEquals(ColumnValue.fromString("blah"), row.getColumn("C1").get(0).getValue());
            assertEquals(ColumnValue.fromLong(123L), row.getColumn("C2").get(0).getValue());
            assertEquals(ColumnValue.fromBoolean(true), row.getColumn("C3").get(0).getValue());
            assertEquals(ColumnValue.fromBoolean(false), row.getColumn("C4").get(0).getValue());
            assertEquals(ColumnValue.fromDouble(3.14), row.getColumn("C5").get(0).getValue());
            assertEquals(ColumnValue.fromBinary(binaryValue), row.getColumn("C6").get(0).getValue());
        }
    }

    private void createAndInitTableForGetRangeTest(String tableName) {
        byte[] binaryValue = new byte[1];
        binaryValue[0] = (byte) 0xff;

        List<PrimaryKeySchema> pk = new ArrayList<PrimaryKeySchema>();
        pk.add(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK2", PrimaryKeyType.STRING));
        pk.add(new PrimaryKeySchema("PK3", PrimaryKeyType.INTEGER));

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumns(pk);

        CapacityUnit capacityUnit = new CapacityUnit(0, 0);

        ReservedThroughput reservedThroughput = new ReservedThroughput(capacityUnit);

        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(Integer.MAX_VALUE);

        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, tableOptions);
        createTableRequest.setReservedThroughput(reservedThroughput);

        ots.createTable(createTableRequest);
        Utils.waitForPartitionLoad(tableName);

        // PrimaryKey
        List<PrimaryKeyColumn> primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        PrimaryKey primaryKey = new PrimaryKey(primaryKeyList);

        // Columns
        List<Column> columnList = new ArrayList<Column>();
        columnList.add(new Column("C1", ColumnValue.fromString("blah")));
        columnList.add(new Column("C2", ColumnValue.fromLong(123L)));
        columnList.add(new Column("C3", ColumnValue.fromBoolean(true)));
        columnList.add(new Column("C4", ColumnValue.fromBoolean(false)));
        columnList.add(new Column("C5", ColumnValue.fromDouble(3.14)));
        columnList.add(new Column("C6", ColumnValue.fromBinary(binaryValue)));

        // PutRow
        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
        rowPutChange.addColumns(columnList);

        PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);

        ots.putRow(putRowRequest);

    }

    /*
     * 分别测试PK个数为1，2，3，4的4个表，GetRange中包含的row个数为0的情况，
     * 期望返回为空，CU消耗为(1, 0)
     */
    @Test
    public void testCaseGetRangeFromEmptyTable() throws Exception {
        List<PrimaryKeyColumn> primaryKeyList;

        // One PK: String
        tableName = "TableNameOnePK";
        createTableWithOnePrimaryKey(tableName);
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("1")));
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("2")));
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        GetRangeRequest getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        GetRangeResponse getRangeResult = ots.getRange(getRangeRequest);
        //assertEquals(new CapacityUnit(0, 0), getRangeResult.getConsumedCapacity().getCapacityUnit());
        assertEquals(0, getRangeResult.getRows().size());

        // Two PK: String Integer
        tableName = "TableNameTwoPK";
        createTableWithTwoPrimaryKeys(tableName);
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("1")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("2")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MAX));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        getRangeResult = ots.getRange(getRangeRequest);
        //assertEquals(new CapacityUnit(0, 0), getRangeResult.getConsumedCapacity().getCapacityUnit());
        assertEquals(0, getRangeResult.getRows().size());

        // Three PK: String Integer Integer
        tableName = "TableNameThreePK";
        createTableWithThreePrimaryKeys(tableName);
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("1")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MIN));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("2")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MAX));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MAX));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        getRangeResult = ots.getRange(getRangeRequest);
        //assertEquals(new CapacityUnit(0, 0), getRangeResult.getConsumedCapacity().getCapacityUnit());
        assertEquals(0, getRangeResult.getRows().size());

        // Four PK: String Integer Integer Integer
        tableName = "TableNameFourPK";
        createTableWithFourPrimaryKeys(tableName);
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("1")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MIN));
        primaryKeyList.add(new PrimaryKeyColumn("PK4", PrimaryKeyValue.INF_MIN));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("2")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MAX));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MAX));
        primaryKeyList.add(new PrimaryKeyColumn("PK4", PrimaryKeyValue.INF_MAX));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);
        rangeRowQueryCriteria.setDirection(Direction.FORWARD);
        rangeRowQueryCriteria.addColumnsToGet("C1");
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
        rangeRowQueryCriteria.setMaxVersions(Integer.MAX_VALUE);
        getRangeRequest = new GetRangeRequest(rangeRowQueryCriteria);
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);

        getRangeResult = ots.getRange(getRangeRequest);
        //assertEquals(new CapacityUnit(0, 0), getRangeResult.getConsumedCapacity().getCapacityUnit());
        assertEquals(0, getRangeResult.getRows().size());
    }


    /*
     * 一个表有3个PK，类型分别是STRING, STRING, INTEGER，
     * 测试range:
     * ('A' 'A' 10, 'A' 'A' 10),
     * ('A' 'A' 10, 'A' 'A' 11),
     * ('A' 'A' 10, 'A' 'A' 9)（出错）,
     * ('A' 'A' MAX, 'A' 'B' MIN),
     * ('A' MIN 10, 'B' MAX 2)，
     * 构造数据让每个区间都有值。
     */
    @Test
    public void testCaseGetRangeWithoutPartition() throws Exception {
        createAndInitTableForGetRangeTest(tableName);

        // ('A' 'A' 10, 'A' 'A' 10)（出错）
        List<PrimaryKeyColumn> primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        PrimaryKey inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        PrimaryKey exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);
        try {
            validateGetRangeWithoutParition(tableName, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 1);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Begin key must less than end key in FORWARD", 400, e);
        }

        // ('A' 'A' 10, 'A' 'A' 11)
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(11L)));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        validateGetRangeWithoutParition(tableName, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 1);

        // ('A' 'A' 10, 'A' 'A' 9)（出错）
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(9L)));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);
        try {
            validateGetRangeWithoutParition(tableName, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 1);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertTableStoreException(ErrorCode.INVALID_PARAMETER, "Begin key must less than end key in FORWARD", 400, e);
        }

        // ('A' 'A' MAX, 'A' 'B' MIN)
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MAX));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.fromString("B")));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.INF_MIN));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        validateGetRangeWithoutParition(tableName, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 0);

        // ('A' MIN 10, 'B' MAX 2)
        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("A")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(10L)));
        inclusiveStartPrimaryKey = new PrimaryKey(primaryKeyList);

        primaryKeyList = new ArrayList<PrimaryKeyColumn>();
        primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("B")));
        primaryKeyList.add(new PrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MAX));
        primaryKeyList.add(new PrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(2L)));
        exclusiveEndPrimaryKey = new PrimaryKey(primaryKeyList);

        validateGetRangeWithoutParition(tableName, inclusiveStartPrimaryKey, exclusiveEndPrimaryKey, 1);
    }

    /*
     * 多线程同时BatchWriteRow一行构造锁冲突错误，期望BatchWriteRow返回的单个item结果中出现RowOperationConflit
     */
    @Test
    public void testCaseBatchWriteRowSucceedWhenAllItemFailedBackend() throws Exception {
        createTableWithOnePrimaryKeyString("TableForFailTest");
        int runnerNumber = 10;
        boolean hasConflict = false;

        List<BatchWriteRowRunner> runnerList = new ArrayList<BatchWriteRowRunner>();
        for (int i = 1; i <= runnerNumber; i++) {
            runnerList.add(new BatchWriteRowRunner(i));
        }

        ExecutorService pool = Executors.newFixedThreadPool(runnerNumber);
        for (BatchWriteRowRunner runner : runnerList) {
            pool.execute(runner);
        }

        pool.shutdown();
        while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {}

        for (BatchWriteRowRunner runner : runnerList) {
            if (runner.getErrorMessage() != null) {
                hasConflict = true;
                assertEquals("Data is being modified by the other request.", runner.getErrorMessage());
                assertEquals(ErrorCode.ROW_OPERATION_CONFLICT, runner.getErrorCode());
            }
        }
        // TODO restore this assert when TxnMonitor is per-partition
        // assertEquals(false, hasConflict);
    }

    public class BatchWriteRowRunner implements Runnable {
        final int value;
        String errorMessage;
        String errorCode;

        public BatchWriteRowRunner(int value) {
            this.value = value;
        }

        public String getErrorMessage() {
            return this.errorMessage;
        }

        public String getErrorCode() {
            return this.errorCode;
        }

        @Override
        public void run() {
            // BatchWriteRow
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            // PrimaryKey
            List<PrimaryKeyColumn> primaryKeyList = new ArrayList<PrimaryKeyColumn>();
            primaryKeyList.add(new PrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("blah")));
            PrimaryKey primaryKey = new PrimaryKey(primaryKeyList);

            RowPutChange newRowPutChange = new RowPutChange("TableForFailTest", primaryKey);
            newRowPutChange.addColumn(new Column("C1", ColumnValue.fromLong((long) value)));
            batchWriteRowRequest.addRowChange(newRowPutChange);

            BatchWriteRowResponse batchWriteRowResult = ots.batchWriteRow(batchWriteRowRequest);
            List<BatchWriteRowResponse.RowResult> rowResultList = batchWriteRowResult.getRowStatus("TableForFailTest");
            assertEquals(1, rowResultList.size());
            if (rowResultList.get(0).getError() != null) {
                this.errorMessage = rowResultList.get(0).getError().getMessage();
                this.errorCode = rowResultList.get(0).getError().getCode();
            } else {
                this.errorMessage = null;
                this.errorCode = null;
            }
        }
    }
}
