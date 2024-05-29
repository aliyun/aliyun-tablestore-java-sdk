package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class TimeseriesTest {

    static String testTable = "SDKTestTimeseriesTable";
    static TimeseriesClient client = null;

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesTest.class);
    private static long waitTableInit = 60 * 1000;
    private static long waitSearchIndexSync = 60 * 1000;
    private static boolean createTableBeforeTest = true; // for local test
    private static boolean deleteTableAfterTest = true; // for local test

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new TimeseriesClient(endPoint, accessId, accessKey, instanceName);

        if (createTableBeforeTest) {
            ListTimeseriesTableResponse listTimeseriesTableResponse = client.listTimeseriesTable();
            for (String table : listTimeseriesTableResponse.getTimeseriesTableNames()) {
                client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(table));
            }

            CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(new TimeseriesTableMeta(testTable));
            client.createTimeseriesTable(request);
            LOG.warn("sleep " + waitTableInit + "ms after create table...");
            try {
                Thread.sleep(waitTableInit);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @AfterClass
    public static void afterClass() {
        if (deleteTableAfterTest) {
            ListTimeseriesTableResponse listTimeseriesTableResponse = client.listTimeseriesTable();
            for (String table : listTimeseriesTableResponse.getTimeseriesTableNames()) {
                client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(table));
            }
        }
        client.shutdown();
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testTableOperations() throws Exception {
        ListTimeseriesTableResponse listTimeseriesTableResponse = client.listTimeseriesTable();
        Assert.assertEquals(1, listTimeseriesTableResponse.getTimeseriesTableNames().size());
        TimeseriesTableMeta tableMeta = listTimeseriesTableResponse.getTimeseriesTableMetas().get(0);
        Assert.assertEquals(testTable, tableMeta.getTimeseriesTableName());
        Assert.assertEquals("CREATED", tableMeta.getStatus());
        Assert.assertEquals(-1, tableMeta.getTimeseriesTableOptions().getTimeToLive());

        DescribeTimeseriesTableResponse describeTimeseriesTableResponse = client.describeTimeseriesTable(new DescribeTimeseriesTableRequest(testTable));
        tableMeta = describeTimeseriesTableResponse.getTimeseriesTableMeta();
        Assert.assertEquals(testTable, tableMeta.getTimeseriesTableName());
        Assert.assertEquals("CREATED", tableMeta.getStatus());
        Assert.assertEquals(-1, tableMeta.getTimeseriesTableOptions().getTimeToLive());

        UpdateTimeseriesTableRequest updateTimeseriesTableRequest = new UpdateTimeseriesTableRequest(testTable);
        updateTimeseriesTableRequest.setTimeseriesTableOptions(new TimeseriesTableOptions(8640000));
        client.updateTimeseriesTable(updateTimeseriesTableRequest);

        describeTimeseriesTableResponse = client.describeTimeseriesTable(new DescribeTimeseriesTableRequest(testTable));
        tableMeta = describeTimeseriesTableResponse.getTimeseriesTableMeta();
        Assert.assertEquals(testTable, tableMeta.getTimeseriesTableName());
        Assert.assertEquals("CREATED", tableMeta.getStatus());
        Assert.assertEquals(8640000, tableMeta.getTimeseriesTableOptions().getTimeToLive());

        updateTimeseriesTableRequest = new UpdateTimeseriesTableRequest(testTable);
        updateTimeseriesTableRequest.setTimeseriesTableOptions(new TimeseriesTableOptions(-1));
        client.updateTimeseriesTable(updateTimeseriesTableRequest);

        describeTimeseriesTableResponse = client.describeTimeseriesTable(new DescribeTimeseriesTableRequest(testTable));
        tableMeta = describeTimeseriesTableResponse.getTimeseriesTableMeta();
        Assert.assertEquals(testTable, tableMeta.getTimeseriesTableName());
        Assert.assertEquals("CREATED", tableMeta.getStatus());
        Assert.assertEquals(-1, tableMeta.getTimeseriesTableOptions().getTimeToLive());
    }

    @Test
    public void testRestrictions() {
        List<String> validMeasurements = Arrays.asList("温度", "温度.abc", "123Test", "_@?%$A");
        for (String m : validMeasurements) {
            TimeseriesKey timeseriesKey = new TimeseriesKey(m, "source_" + new Random().nextInt(1000000));
            TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000);
            timeseriesRow.addField("value", ColumnValue.fromDouble(1.1));
            List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
            rows.add(timeseriesRow);

            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
            putTimeseriesDataRequest.setRows(rows);
            PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
            Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());
        }

        List<String> invalidMeasurements = Arrays.asList("温度\t", "温度.abc\n", " ", "##AA##", "aa bb");
        for (String m : invalidMeasurements) {
            TimeseriesKey timeseriesKey = new TimeseriesKey(m, "source_" + new Random().nextInt(1000000));
            TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000);
            timeseriesRow.addField("value", ColumnValue.fromDouble(1.1));
            List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
            rows.add(timeseriesRow);

            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
            putTimeseriesDataRequest.setRows(rows);
            try {
                PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
                fail();
            } catch (TableStoreException ex) {
                Assert.assertEquals("OTSParameterInvalid", ex.getErrorCode());
            }
        }

        List<String> validTagValues = Arrays.asList("aa_", "123", "@$?A", "杭州", "阿里云tablestore", "aa bb 阿里 ots", "\t\n");
        for (String tagValue : validTagValues) {
            HashMap<String, String> tags = new HashMap<String, String>();
            tags.put("tag", tagValue);
            TimeseriesKey timeseriesKey = new TimeseriesKey("tag_test",
                    "source_" + new Random().nextInt(1000000), tags);
            TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000);
            timeseriesRow.addField("value", ColumnValue.fromDouble(1.1));
            List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
            rows.add(timeseriesRow);

            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
            putTimeseriesDataRequest.setRows(rows);
            PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
            Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());
        }

        List<String> invalidTagValues = Arrays.asList("=", "\"", "aa=a", "zz\"", "aa bb 阿里\\\"\" ots");
        for (String tagValue : invalidTagValues) {
            HashMap<String, String> tags = new HashMap<String, String>();
            tags.put("tag", tagValue);
            TimeseriesKey timeseriesKey = new TimeseriesKey("tag_test",
                    "source_" + new Random().nextInt(1000000), tags);
            TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000);
            timeseriesRow.addField("value", ColumnValue.fromDouble(1.1));
            List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
            rows.add(timeseriesRow);

            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
            putTimeseriesDataRequest.setRows(rows);
            try {
                PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
                fail();
            } catch (TableStoreException ex) {
                assertEquals("OTSParameterInvalid", ex.getErrorCode());
            }
        }
    }

    @Test
    public void testDataOperations() throws InterruptedException {
        List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
        Map<String, String> tags = new HashMap<String, String>();
        for (int i = 0; i < 10; i++) {
            tags.put("tag" + i, "value" + i);
        }
        for (int i = 0; i < 100; i++) {
            TimeseriesKey timeseriesKey = new TimeseriesKey("test_measure", "source" + i, tags);
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
            for (int j = 0; j < 10; j++) {
                row.addField("field" + j, ColumnValue.fromString("value" + j));
            }
            rows.add(row);
        }
        PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
        putTimeseriesDataRequest.setRows(rows);
        PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
        Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());

        GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
        getTimeseriesDataRequest.setTimeseriesKey(new TimeseriesKey("test_measure", "source5", tags));
        getTimeseriesDataRequest.setTimeRange(0, System.currentTimeMillis() * 1000 + 1000);
        GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
        Assert.assertEquals(1, getTimeseriesDataResponse.getRows().size());
        Assert.assertNull(getTimeseriesDataResponse.getNextToken());
        TimeseriesRow row = getTimeseriesDataResponse.getRows().get(0);
        Assert.assertEquals("test_measure", row.getTimeseriesKey().getMeasurementName());
        Assert.assertEquals("source5", row.getTimeseriesKey().getDataSource());
        Assert.assertEquals(tags, row.getTimeseriesKey().getTags());

        try {
            Thread.sleep(waitSearchIndexSync);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        {
            QueryTimeseriesMetaRequest queryTimeseriesMetaRequest = new QueryTimeseriesMetaRequest(testTable);
            CompositeMetaQueryCondition compositeMetaQueryCondition = new CompositeMetaQueryCondition(MetaQueryCompositeOperator.OP_AND);
            compositeMetaQueryCondition.addSubCondition(new MeasurementMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "test"));
            compositeMetaQueryCondition.addSubCondition(new DataSourceMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "source1"));
            compositeMetaQueryCondition.addSubCondition(new TagMetaQueryCondition(MetaQuerySingleOperator.OP_GREATER_EQUAL, "tag5", "value5"));
            queryTimeseriesMetaRequest.setCondition(compositeMetaQueryCondition);
            queryTimeseriesMetaRequest.setGetTotalHits(true);
            QueryTimeseriesMetaResponse queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
            Assert.assertEquals(11, queryTimeseriesMetaResponse.getTimeseriesMetas().size());
            Assert.assertEquals(11, queryTimeseriesMetaResponse.getTotalHits());
            Assert.assertNull(queryTimeseriesMetaResponse.getNextToken());
            Assert.assertTrue(queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getAttributes().isEmpty());
            Assert.assertTrue(queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getUpdateTimeInUs() > 0);
            Assert.assertEquals("test_measure", queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getMeasurementName());
            Assert.assertTrue(queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getDataSource().startsWith("source1"));
            Assert.assertEquals(tags, queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getTags());
        }

        Map<String, String> attrs = new HashMap<String, String>();
        attrs.put("attr1", "attr_value1");
        attrs.put("attr2", "attr_value2");
        List<TimeseriesMeta> timeseriesMetaList = new ArrayList<TimeseriesMeta>();
        for (int i = 0; i < 10; i++) {
            TimeseriesKey timeseriesKey = new TimeseriesKey("test_measure", "update_source" + i, tags);
            TimeseriesMeta meta = new TimeseriesMeta(timeseriesKey);
            meta.setAttributes(attrs);
            timeseriesMetaList.add(meta);
        }
        UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest = new UpdateTimeseriesMetaRequest(testTable);
        updateTimeseriesMetaRequest.setMetas(timeseriesMetaList);
        UpdateTimeseriesMetaResponse updateTimeseriesMetaResponse = client.updateTimeseriesMeta(updateTimeseriesMetaRequest);
        Assert.assertTrue(updateTimeseriesMetaResponse.isAllSuccess());

        try {
            Thread.sleep(waitSearchIndexSync);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        {
            QueryTimeseriesMetaRequest queryTimeseriesMetaRequest = new QueryTimeseriesMetaRequest(testTable);
            CompositeMetaQueryCondition compositeMetaQueryCondition = new CompositeMetaQueryCondition(MetaQueryCompositeOperator.OP_AND);
            compositeMetaQueryCondition.addSubCondition(new MeasurementMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "test"));
            compositeMetaQueryCondition.addSubCondition(new DataSourceMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "update_source"));
            compositeMetaQueryCondition.addSubCondition(new AttributeMetaQueryCondition(MetaQuerySingleOperator.OP_GREATER_EQUAL, "attr1", "attr_value1"));
            queryTimeseriesMetaRequest.setCondition(compositeMetaQueryCondition);
            queryTimeseriesMetaRequest.setGetTotalHits(true);
            QueryTimeseriesMetaResponse queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
            Assert.assertEquals(10, queryTimeseriesMetaResponse.getTimeseriesMetas().size());
            Assert.assertEquals(10, queryTimeseriesMetaResponse.getTotalHits());
            Assert.assertNull(queryTimeseriesMetaResponse.getNextToken());
            Assert.assertTrue(queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getUpdateTimeInUs() > 0);
            Assert.assertEquals("test_measure", queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getMeasurementName());
            Assert.assertTrue(queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getDataSource().startsWith("update_source"));
            Assert.assertEquals(tags, queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getTimeseriesKey().getTags());
            Assert.assertEquals(attrs, queryTimeseriesMetaResponse.getTimeseriesMetas().get(0).getAttributes());
        }

        {
            QueryTimeseriesMetaRequest queryTimeseriesMetaRequest = new QueryTimeseriesMetaRequest(testTable);
            CompositeMetaQueryCondition compositeMetaQueryCondition = new CompositeMetaQueryCondition(MetaQueryCompositeOperator.OP_AND);
            compositeMetaQueryCondition.addSubCondition(new MeasurementMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "test"));
            compositeMetaQueryCondition.addSubCondition(new DataSourceMetaQueryCondition(MetaQuerySingleOperator.OP_PREFIX, "update_source"));
            compositeMetaQueryCondition.addSubCondition(new AttributeMetaQueryCondition(MetaQuerySingleOperator.OP_GREATER_EQUAL, "attr1", "attr_value1"));
            queryTimeseriesMetaRequest.setCondition(compositeMetaQueryCondition);
            queryTimeseriesMetaRequest.setGetTotalHits(true);
            QueryTimeseriesMetaResponse queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
            Assert.assertEquals(10, queryTimeseriesMetaResponse.getTimeseriesMetas().size());
            Assert.assertEquals(10, queryTimeseriesMetaResponse.getTotalHits());

            DeleteTimeseriesMetaRequest deleteTimeseriesMetaRequest = new DeleteTimeseriesMetaRequest(testTable);
            List<TimeseriesKey> keyList = new ArrayList<TimeseriesKey>();
            for (int i = 0; i < queryTimeseriesMetaResponse.getTimeseriesMetas().size(); i++) {
                keyList.add(queryTimeseriesMetaResponse.getTimeseriesMetas().get(i).getTimeseriesKey());
            }
            deleteTimeseriesMetaRequest.setTimeseriesKeys(keyList);

            DeleteTimeseriesMetaResponse deleteTimeseriesMetaResponse =
                    client.deleteTimeseriesMeta(deleteTimeseriesMetaRequest);
            Assert.assertTrue(deleteTimeseriesMetaResponse.isAllSuccess());

            Thread.sleep(waitSearchIndexSync);

            queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
            Assert.assertEquals(0, queryTimeseriesMetaResponse.getTimeseriesMetas().size());
            Assert.assertEquals(0, queryTimeseriesMetaResponse.getTotalHits());
        }

    }

    @Test
    public void testPutTimeseriesDataIgnoreMetaUpdate() throws InterruptedException {
        List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
        Map<String, String> tags = new HashMap<String, String>();
        for (int i = 0; i < 10; i++) {
            TimeseriesKey timeseriesKey = new TimeseriesKey("not_update_meta", "source" + i, tags);
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, System.currentTimeMillis() * 1000 + i);
            for (int j = 0; j < 10; j++) {
                row.addField("field" + j, ColumnValue.fromString("value" + j));
            }
            rows.add(row);
        }
        PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
        putTimeseriesDataRequest.setRows(rows);
        putTimeseriesDataRequest.setMetaUpdateMode(PutTimeseriesDataRequest.MetaUpdateMode.IGNORE);
        PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
        Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());

        GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
        getTimeseriesDataRequest.setTimeseriesKey(new TimeseriesKey("not_update_meta", "source5", tags));
        getTimeseriesDataRequest.setTimeRange(0, System.currentTimeMillis() * 1000 + 1000);
        GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
        Assert.assertEquals(1, getTimeseriesDataResponse.getRows().size());
        Assert.assertNull(getTimeseriesDataResponse.getNextToken());
        TimeseriesRow row = getTimeseriesDataResponse.getRows().get(0);
        Assert.assertEquals("not_update_meta", row.getTimeseriesKey().getMeasurementName());
        Assert.assertEquals("source5", row.getTimeseriesKey().getDataSource());

        try {
            Thread.sleep(waitSearchIndexSync);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        {
            QueryTimeseriesMetaRequest queryTimeseriesMetaRequest = new QueryTimeseriesMetaRequest(testTable);
            queryTimeseriesMetaRequest.setCondition(
                    new MeasurementMetaQueryCondition(MetaQuerySingleOperator.OP_EQUAL, "not_update_meta"));
            queryTimeseriesMetaRequest.setGetTotalHits(true);
            QueryTimeseriesMetaResponse queryTimeseriesMetaResponse = client.queryTimeseriesMeta(queryTimeseriesMetaRequest);
            Assert.assertEquals(0, queryTimeseriesMetaResponse.getTimeseriesMetas().size());
            Assert.assertEquals(0, queryTimeseriesMetaResponse.getTotalHits());
        }
    }

    @Test
    public void testGetTimeseriesData() {
        List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
        long timeStart = System.currentTimeMillis() * 1000;
        TimeseriesKey timeseriesKey = new TimeseriesKey("test_getts", "");
        for (int i = 0; i < 100; i++) {
            TimeseriesRow row = new TimeseriesRow(timeseriesKey, timeStart + i);
            for (int j = 0; j < 10; j++) {
                row.addField("string_" + j, ColumnValue.fromString("value" + j));
            }
            for (int j = 0; j < 10; j++) {
                row.addField("long_" + j, ColumnValue.fromLong(j));
            }
            for (int j = 0; j < 10; j++) {
                row.addField("binary_" + j, ColumnValue.fromBinary(("value" + j).getBytes()));
            }
            for (int j = 0; j < 10; j++) {
                row.addField("double_" + j, ColumnValue.fromDouble(j * 1.01));
            }
            for (int j = 0; j < 10; j++) {
                row.addField("bool_" + j, ColumnValue.fromBoolean(true));
            }
            rows.add(row);
        }
        PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
        putTimeseriesDataRequest.setRows(rows);
        PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
        Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());

        {
            GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
            getTimeseriesDataRequest.setTimeseriesKey(timeseriesKey);
            getTimeseriesDataRequest.setTimeRange(timeStart, timeStart + 1000000);
            GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
            Assert.assertEquals(100, getTimeseriesDataResponse.getRows().size());
            for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                assertEquals(timeStart + i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
            }
        }

        {
            GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
            getTimeseriesDataRequest.setTimeseriesKey(timeseriesKey);
            getTimeseriesDataRequest.setTimeRange(timeStart, timeStart + 1000000);
            getTimeseriesDataRequest.setLimit(10);
            GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
            Assert.assertEquals(10, getTimeseriesDataResponse.getRows().size());
            for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                assertEquals(timeStart + i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
            }
            Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
        }

        {
            GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
            getTimeseriesDataRequest.setTimeseriesKey(timeseriesKey);
            getTimeseriesDataRequest.setTimeRange(timeStart, timeStart + 1000000);
            getTimeseriesDataRequest.setLimit(10);
            getTimeseriesDataRequest.addFieldToGet("string_1", ColumnType.STRING);
            getTimeseriesDataRequest.addFieldToGet("string_2", ColumnType.STRING);
            getTimeseriesDataRequest.addFieldToGet("string_3", ColumnType.DOUBLE); // wrong type
            getTimeseriesDataRequest.addFieldToGet("double_3", ColumnType.DOUBLE);
            getTimeseriesDataRequest.addFieldToGet("double_4", ColumnType.INTEGER); // wrong type
            getTimeseriesDataRequest.addFieldToGet("long_1", ColumnType.INTEGER);
            getTimeseriesDataRequest.addFieldToGet("binary_5", ColumnType.BINARY);
            getTimeseriesDataRequest.addFieldToGet("bool_2", ColumnType.BOOLEAN);
            GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
            Assert.assertEquals(10, getTimeseriesDataResponse.getRows().size());
            for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                TimeseriesRow row = getTimeseriesDataResponse.getRows().get(i);
                assertEquals(timeStart + i, row.getTimeInUs());
                assertEquals(6, row.getFields().size());
                assertEquals("value1", row.getFields().get("string_1").asString());
                assertEquals("value2", row.getFields().get("string_2").asString());
                assertEquals(3*1.01, row.getFields().get("double_3").asDouble(), 0.01);
                assertEquals(1, row.getFields().get("long_1").asLong());
                assertArrayEquals(("value" + 5).getBytes(), row.getFields().get("binary_5").asBinary());
                assertTrue(row.getFields().get("bool_2").asBoolean());
            }
            Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
        }

        {
            GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
            getTimeseriesDataRequest.setTimeseriesKey(timeseriesKey);
            getTimeseriesDataRequest.setTimeRange(timeStart + 20, timeStart + 1000000);
            int limit = 1 + new Random().nextInt(10);
            getTimeseriesDataRequest.setLimit(limit);
            GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
            Assert.assertEquals(limit, getTimeseriesDataResponse.getRows().size());
            for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                assertEquals(timeStart + 20 + i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
            }
            Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
            long timeNext = getTimeseriesDataResponse.getRows().get(limit - 1).getTimeInUs() + 1;
            while (true) {
                getTimeseriesDataRequest.setNextToken(getTimeseriesDataResponse.getNextToken());
                getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
                for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                    assertEquals(timeNext + i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
                }
                Assert.assertTrue(getTimeseriesDataResponse.getRows().size() <= limit);
                timeNext = getTimeseriesDataResponse.getRows().get(getTimeseriesDataResponse.getRows().size() - 1).getTimeInUs() + 1;
                if (timeNext < timeStart + 100) {
                    Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
                } else {
                    break;
                }
            }
        }

        {
            GetTimeseriesDataRequest getTimeseriesDataRequest = new GetTimeseriesDataRequest(testTable);
            getTimeseriesDataRequest.setTimeseriesKey(timeseriesKey);
            getTimeseriesDataRequest.setTimeRange(timeStart + 20, timeStart + 1000000);
            int limit = 1 + new Random().nextInt(10);
            getTimeseriesDataRequest.setLimit(limit);
            getTimeseriesDataRequest.setBackward(true);
            GetTimeseriesDataResponse getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
            Assert.assertEquals(limit, getTimeseriesDataResponse.getRows().size());
            for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                assertEquals(timeStart + 99 - i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
            }
            Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
            long timeNext = getTimeseriesDataResponse.getRows().get(limit - 1).getTimeInUs() - 1;
            while (true) {
                getTimeseriesDataRequest.setNextToken(getTimeseriesDataResponse.getNextToken());
                getTimeseriesDataResponse = client.getTimeseriesData(getTimeseriesDataRequest);
                for (int i = 0 ; i < getTimeseriesDataResponse.getRows().size(); i++) {
                    assertEquals(timeNext - i, getTimeseriesDataResponse.getRows().get(i).getTimeInUs());
                }
                Assert.assertTrue(getTimeseriesDataResponse.getRows().size() <= limit);
                timeNext = getTimeseriesDataResponse.getRows().get(getTimeseriesDataResponse.getRows().size() - 1).getTimeInUs() - 1;
                if (timeNext >= getTimeseriesDataRequest.getBeginTimeInUs()) {
                    Assert.assertNotNull(getTimeseriesDataResponse.getNextToken());
                } else {
                    break;
                }
            }
        }

    }

}
