package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TimeseriesCustomPrimaryKeysTest {

    static TimeseriesClient client = null;
    static SyncClient tableStoreClient = null;

    // unique per run: fixed names get deleted by / collide with concurrent gate shards
    static final String TABLE_CUSTOM_PK = com.alicloud.openservices.tablestore.common.OTSHelper.generateUniqueTableName("test_custom_primary_keys");
    static final String TABLE_CUSTOM_PK_META = com.alicloud.openservices.tablestore.common.OTSHelper.generateUniqueTableName("test_custom_primary_keys_meta");
    static final String TABLE_CUSTOM_PK_BATCH_WRITE = com.alicloud.openservices.tablestore.common.OTSHelper.generateUniqueTableName("test_custom_primary_keys_batch_write");
    static final String TABLE_NO_MEASUREMENTS = com.alicloud.openservices.tablestore.common.OTSHelper.generateUniqueTableName("test_no_measurements");

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new TimeseriesClient(endPoint, accessId, accessKey, instanceName);
        tableStoreClient = new SyncClient(endPoint, accessId, accessKey, instanceName);
    }

    @AfterClass
    public static void afterClass() {
        deleteTable(TABLE_CUSTOM_PK);
        deleteTable(TABLE_CUSTOM_PK_META);
        deleteTable(TABLE_CUSTOM_PK_BATCH_WRITE);
        deleteTable(TABLE_NO_MEASUREMENTS);
        client.shutdown();
        tableStoreClient.shutdown();
    }

    static void deleteTable(String tableName) {
        try {
            client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(tableName));
        } catch (Exception ignore) {}
    }

    @Test
    public void testCustomPrimaryKeys() throws InterruptedException {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta(TABLE_CUSTOM_PK);
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        meta.addTimeseriesKey("_m_name");
        meta.addTimeseriesKey("_tags");
        meta.addTimeseriesKey("region");
        meta.addFieldPrimaryKey("cores", PrimaryKeyType.INTEGER);
        meta.addFieldPrimaryKey("frequency", PrimaryKeyType.STRING);
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        com.alicloud.openservices.tablestore.common.OTSHelper.waitForTimeseriesTableReady(client, TABLE_CUSTOM_PK);
        // describe table
        DescribeTimeseriesTableRequest describeTimeseriesTableRequest = new DescribeTimeseriesTableRequest(TABLE_CUSTOM_PK);
        DescribeTimeseriesTableResponse describeTimeseriesTableResponse = client.describeTimeseriesTable(describeTimeseriesTableRequest);
        List<String> primaryKeys = describeTimeseriesTableResponse.getTimeseriesTableMeta().getTimeseriesKeys();
        Assert.assertEquals(3, primaryKeys.size());
        Assert.assertEquals("_m_name", primaryKeys.get(0));
        Assert.assertEquals("_tags", primaryKeys.get(1));
        Assert.assertEquals("region", primaryKeys.get(2));
        List<PrimaryKeySchema> primaryKeyFields = describeTimeseriesTableResponse.getTimeseriesTableMeta().getFieldPrimaryKeys();
        Assert.assertEquals(2, primaryKeyFields.size());
        Assert.assertEquals("cores", primaryKeyFields.get(0).getName());
        Assert.assertEquals(PrimaryKeyType.INTEGER, primaryKeyFields.get(0).getType());
        Assert.assertEquals("frequency", primaryKeyFields.get(1).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyFields.get(1).getType());

        // list table
        ListTimeseriesTableResponse listTimeseriesTableResponse = client.listTimeseriesTable();
        List<TimeseriesTableMeta> tableMetas = listTimeseriesTableResponse.getTimeseriesTableMetas();
        boolean hasTestPrimaryKeyFieldsTable = false;
        for (TimeseriesTableMeta tableMeta : tableMetas) {
            if (tableMeta.getTimeseriesTableName().equals(TABLE_CUSTOM_PK)) {
                hasTestPrimaryKeyFieldsTable = true;
                primaryKeys = tableMeta.getTimeseriesKeys();
                Assert.assertEquals(3, primaryKeys.size());
                Assert.assertEquals("_m_name", primaryKeys.get(0));
                Assert.assertEquals("_tags", primaryKeys.get(1));
                Assert.assertEquals("region", primaryKeys.get(2));
                primaryKeyFields = tableMeta.getFieldPrimaryKeys();
                Assert.assertEquals(2, primaryKeyFields.size());
                Assert.assertEquals("cores", primaryKeyFields.get(0).getName());
                Assert.assertEquals(PrimaryKeyType.INTEGER, primaryKeyFields.get(0).getType());
                Assert.assertEquals("frequency", primaryKeyFields.get(1).getName());
                Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyFields.get(1).getType());
            }
        }
        Assert.assertTrue(hasTestPrimaryKeyFieldsTable);

        // put data
        PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(TABLE_CUSTOM_PK);
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("region", "hangzhou");
        tags.put("vendor", "intel");
        TimeseriesKey primaryKey = new TimeseriesKey("cpu", "", tags);
        for (int i = 0; i < 10; i++) {
            TimeseriesRow row = new TimeseriesRow(primaryKey);
            row.setTimeInUs(System.currentTimeMillis() * 1000);
            row.addField("cores", ColumnValue.fromLong(8));
            row.addField("frequency", ColumnValue.fromString(i + "GHz"));
            row.addField("load1", ColumnValue.fromDouble(0.5));
            putTimeseriesDataRequest.addRow(row);
        }
        PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
        Assert.assertEquals(0, putTimeseriesDataResponse.getFailedRows().size());

        // get data
        GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(TABLE_CUSTOM_PK);
        getTimeseriesDataRequest.setTimeseriesKey(primaryKey);
        getTimeseriesDataRequest.setTimeRange(0, System.currentTimeMillis() * 1000);
        GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
        List<TimeseriesRow> rows = getTimeseriesDataResponse.getRows();
        Assert.assertEquals(10, rows.size());
        for (int i = 0; i < 10; i++) {
            TimeseriesKey key = rows.get(i).getTimeseriesKey();
            Assert.assertEquals("cpu", key.getMeasurementName());
            Assert.assertEquals("", key.getDataSource());
            tags = key.getTags();
            Assert.assertEquals(2, tags.size());
            Assert.assertEquals("hangzhou", tags.get("region"));
            Assert.assertEquals("intel", tags.get("vendor"));
            Map<String, ColumnValue> fields = rows.get(i).getFields();
            Assert.assertEquals(3, fields.size());
            Assert.assertEquals(8L, fields.get("cores").getValue());
            Assert.assertEquals(i + "GHz", fields.get("frequency").getValue());
            Assert.assertEquals(0.5, fields.get("load1").getValue());
        }

        // scan data
        ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(TABLE_CUSTOM_PK);
        scanTimeseriesDataRequest.setTimeRange(0, System.currentTimeMillis() * 1000);
        ScanTimeseriesDataResponse scanTimeseriesDataResponse = client.scanTimeseriesData(scanTimeseriesDataRequest);
        rows = scanTimeseriesDataResponse.getRows();
        Assert.assertEquals(10, rows.size());
        for (int i = 0; i < 10; i++) {
            Map<String, ColumnValue> fields = rows.get(i).getFields();
            Assert.assertEquals(3, fields.size());
            Assert.assertEquals(8L, fields.get("cores").getValue());
            Assert.assertEquals(i + "GHz", fields.get("frequency").getValue());
            Assert.assertEquals(0.5, fields.get("load1").getValue());
        }
    }

    @Ignore
    @Test
    public void testCustomPrimaryKeysMeta() throws InterruptedException {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta(TABLE_CUSTOM_PK_META);
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        meta.addTimeseriesKey("_m_name");
        meta.addTimeseriesKey("_tags");
        meta.addTimeseriesKey("region");
        meta.addFieldPrimaryKey("cores", PrimaryKeyType.INTEGER);
        meta.addFieldPrimaryKey("frequency", PrimaryKeyType.STRING);
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        com.alicloud.openservices.tablestore.common.OTSHelper.waitForTimeseriesTableReady(client, TABLE_CUSTOM_PK_META);
        // update meta
        UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest = new UpdateTimeseriesMetaRequest(TABLE_CUSTOM_PK_META);
        List<TimeseriesMeta> metas = new ArrayList<TimeseriesMeta>();
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("region", "hangzhou");
        tags.put("vendor", "intel");
        TimeseriesKey primaryKey = new TimeseriesKey("cpu", "", tags);
        TimeseriesMeta timeseriesMeta = new TimeseriesMeta(primaryKey);
        metas.add(timeseriesMeta);
        tags.put("region", "beijing");
        tags.put("vendor", "amd");
        primaryKey = new TimeseriesKey("cpu", "", tags);
        timeseriesMeta = new TimeseriesMeta(primaryKey);
        metas.add(timeseriesMeta);
        updateTimeseriesMetaRequest.setMetas(metas);
        UpdateTimeseriesMetaResponse updateTimeseriesMetaResponse = client.updateTimeseriesMeta(updateTimeseriesMetaRequest);
        Assert.assertEquals(0, updateTimeseriesMetaResponse.getFailedRows().size());

        // wait for meta update
        TimeUnit.SECONDS.sleep(60);

        // get meta
        QueryTimeseriesMetaRequest queryTimeseriesMetaRequest = new QueryTimeseriesMetaRequest(TABLE_CUSTOM_PK_META);
        queryTimeseriesMetaRequest.setCondition(new TagMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, "region", "beijing"));
        QueryTimeseriesMetaResponse queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
        List<TimeseriesMeta> timeseriesMetas = queryTimeseriesMetaResponse.getTimeseriesMetas();
        Assert.assertEquals(1, timeseriesMetas.size());
        Assert.assertEquals("cpu", timeseriesMetas.get(0).getTimeseriesKey().getMeasurementName());
        Assert.assertEquals("", timeseriesMetas.get(0).getTimeseriesKey().getDataSource());
        tags = timeseriesMetas.get(0).getTimeseriesKey().getTags();
        Assert.assertEquals(2, tags.size());
        Assert.assertEquals("beijing", tags.get("region"));
        Assert.assertEquals("amd", tags.get("vendor"));

        // delete meta
        DeleteTimeseriesMetaRequest deleteTimeseriesMetaRequest = new DeleteTimeseriesMetaRequest(TABLE_CUSTOM_PK_META);
        List<TimeseriesKey> pks = new ArrayList<TimeseriesKey>();
        pks.add(primaryKey);
        deleteTimeseriesMetaRequest.setTimeseriesKeys(pks);
        DeleteTimeseriesMetaResponse deleteTimeseriesMetaResponse = client.deleteTimeseriesMeta(deleteTimeseriesMetaRequest);
        Assert.assertEquals(0, deleteTimeseriesMetaResponse.getFailedRows().size());
        TimeUnit.SECONDS.sleep(60);
        queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
        timeseriesMetas = queryTimeseriesMetaResponse.getTimeseriesMetas();
        Assert.assertEquals(0, timeseriesMetas.size());
    }

    @Test
    public void testBatchWrite() throws InterruptedException {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta(TABLE_CUSTOM_PK_BATCH_WRITE);
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        meta.addTimeseriesKey("_m_name");
        meta.addTimeseriesKey("_tags");
        meta.addTimeseriesKey("region");
        meta.addFieldPrimaryKey("cores", PrimaryKeyType.INTEGER);
        meta.addFieldPrimaryKey("frequency", PrimaryKeyType.STRING);
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        com.alicloud.openservices.tablestore.common.OTSHelper.waitForTimeseriesTableReady(client, TABLE_CUSTOM_PK_BATCH_WRITE);
        // describe table
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(TABLE_CUSTOM_PK_BATCH_WRITE + "#timeseries");
        DescribeTableResponse describeTableResponse = tableStoreClient.describeTable(describeTableRequest);
        List<PrimaryKeySchema> primaryKeyList = describeTableResponse.getTableMeta().getPrimaryKeyList();
        Assert.assertEquals(7, primaryKeyList.size());
        Assert.assertEquals("_#h", primaryKeyList.get(0).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyList.get(0).getType());
        Assert.assertEquals("_m_name", primaryKeyList.get(1).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyList.get(1).getType());
        Assert.assertEquals("_tags", primaryKeyList.get(2).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyList.get(2).getType());
        Assert.assertEquals("region", primaryKeyList.get(3).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyList.get(3).getType());
        Assert.assertEquals("_time", primaryKeyList.get(4).getName());
        Assert.assertEquals(PrimaryKeyType.INTEGER, primaryKeyList.get(4).getType());
        Assert.assertEquals("cores", primaryKeyList.get(5).getName());
        Assert.assertEquals(PrimaryKeyType.INTEGER, primaryKeyList.get(5).getType());
        Assert.assertEquals("frequency", primaryKeyList.get(6).getName());
        Assert.assertEquals(PrimaryKeyType.STRING, primaryKeyList.get(6).getType());

        // batch write
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        RowPutChange rowPutChange = new RowPutChange(TABLE_CUSTOM_PK_BATCH_WRITE + "#timeseries");
        rowPutChange.setPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("_#h", PrimaryKeyValue.fromString("")),
                new PrimaryKeyColumn("_m_name", PrimaryKeyValue.fromString("cpu")),
                new PrimaryKeyColumn("_tags", PrimaryKeyValue.fromString("[\"vendor=intel\"]")),
                new PrimaryKeyColumn("region", PrimaryKeyValue.fromString("hangzhou")),
                new PrimaryKeyColumn("_time", PrimaryKeyValue.fromLong(System.currentTimeMillis() * 1000)),
                new PrimaryKeyColumn("cores", PrimaryKeyValue.fromLong(8)),
                new PrimaryKeyColumn("frequency", PrimaryKeyValue.fromString("2.0GHz"))
        }));
        rowPutChange.addColumn("load1:d", ColumnValue.fromDouble(0.5));
        batchWriteRowRequest.addRowChange(rowPutChange);
        BatchWriteRowResponse batchWriteRowResponse = tableStoreClient.batchWriteRow(batchWriteRowRequest);
        Assert.assertEquals(0, batchWriteRowResponse.getFailedRows().size());

        // get range
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(TABLE_CUSTOM_PK_BATCH_WRITE + "#timeseries");
        rangeRowQueryCriteria.setMaxVersions(1);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("_#h", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("_m_name", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("_tags", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("region", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("_time", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("cores", PrimaryKeyValue.INF_MIN),
                new PrimaryKeyColumn("frequency", PrimaryKeyValue.INF_MIN)
        }));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(new PrimaryKey(new PrimaryKeyColumn[]{
                new PrimaryKeyColumn("_#h", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("_m_name", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("_tags", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("region", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("_time", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("cores", PrimaryKeyValue.INF_MAX),
                new PrimaryKeyColumn("frequency", PrimaryKeyValue.INF_MAX)
        }));
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        GetRangeResponse getRangeResponse = tableStoreClient.getRange(getRangeRequest);
        List<Row> rows = getRangeResponse.getRows();
        Assert.assertEquals(1, rows.size());
        PrimaryKey primaryKey = rows.get(0).getPrimaryKey();
        Assert.assertEquals(7, primaryKey.getPrimaryKeyColumns().length);
        Assert.assertEquals(PrimaryKeyValue.fromString("cpu"), primaryKey.getPrimaryKeyColumns()[1].getValue());
        Assert.assertEquals(PrimaryKeyValue.fromString("[\"vendor=intel\"]"), primaryKey.getPrimaryKeyColumns()[2].getValue());
        Assert.assertEquals(PrimaryKeyValue.fromString("hangzhou"), primaryKey.getPrimaryKeyColumns()[3].getValue());
        Assert.assertEquals(PrimaryKeyValue.fromLong(8), primaryKey.getPrimaryKeyColumns()[5].getValue());
        Assert.assertEquals(PrimaryKeyValue.fromString("2.0GHz"), primaryKey.getPrimaryKeyColumns()[6].getValue());
        Column load1 = rows.get(0).getLatestColumn("load1:d");
        Assert.assertEquals(0.5, load1.getValue().asDouble(), 0.0001);
    }

    @Test
    public void testNoMeasurements() throws InterruptedException {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta(TABLE_NO_MEASUREMENTS);
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        meta.addTimeseriesKey("_data_source");
        meta.addTimeseriesKey("_tags");
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        com.alicloud.openservices.tablestore.common.OTSHelper.waitForTimeseriesTableReady(client, TABLE_NO_MEASUREMENTS);

        // put data
        PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(TABLE_NO_MEASUREMENTS);
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("region", "hangzhou");
        tags.put("vendor", "intel");
        TimeseriesKey primaryKey = new TimeseriesKey("", "_data_source", tags);
        for (int i = 0; i < 10; i++) {
            TimeseriesRow row = new TimeseriesRow(primaryKey);
            row.setTimeInUs(System.currentTimeMillis() * 1000 + i);
            row.addField("load1", ColumnValue.fromDouble(0.5));
            putTimeseriesDataRequest.addRow(row);
        }
        PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
        Assert.assertEquals(0, putTimeseriesDataResponse.getFailedRows().size());

        // get data
        GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(TABLE_NO_MEASUREMENTS);
        getTimeseriesDataRequest.setTimeseriesKey(primaryKey);
        getTimeseriesDataRequest.setTimeRange(0, System.currentTimeMillis() * 1000);
        GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
        List<TimeseriesRow> rows = getTimeseriesDataResponse.getRows();
        Assert.assertEquals(10, rows.size());
        for (int i = 0; i < 10; i++) {
            TimeseriesKey key = rows.get(i).getTimeseriesKey();
            Assert.assertEquals("", key.getMeasurementName());
            Assert.assertEquals("_data_source", key.getDataSource());
            tags = key.getTags();
            Assert.assertEquals(2, tags.size());
            Assert.assertEquals("hangzhou", tags.get("region"));
            Assert.assertEquals("intel", tags.get("vendor"));
            Map<String, ColumnValue> fields = rows.get(i).getFields();
            Assert.assertEquals(1, fields.size());
            Assert.assertEquals(0.5, fields.get("load1").getValue());
        }
    }
}
