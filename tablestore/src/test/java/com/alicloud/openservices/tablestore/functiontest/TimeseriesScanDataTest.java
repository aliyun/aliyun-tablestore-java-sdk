package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;

public class TimeseriesScanDataTest {

    static String testTable = "SDKTestScanTimeseriesData";
    static TimeseriesClient client = null;

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesTest.class);
    private static long waitTableInit = 35 * 1000;
    private static boolean createTableBeforeTest = true; // for local test
    private static boolean deleteTableAfterTest = true; // for local test

    private static Map<String, Integer> measurementRowCount = new HashMap<String, Integer>();
    private static int totalRowCount;
    private static TreeMap<Long, Long> timeCounter = new TreeMap<Long, Long>();

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

            loadData();
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

    public static void loadData() {
        Random random = new Random();
        int measurementCount = random.nextInt(10) + 5;
        totalRowCount = 0;
        long rowStartTime = System.currentTimeMillis() * 1000;
        for (int i = 0; i < measurementCount; i++) {
            String measurement = "measurement_" + i;
            int rowCount = random.nextInt(10000) + 1000;
            measurementRowCount.put(measurement, rowCount);
            System.out.printf("%s, %d\n", measurement, rowCount);
            totalRowCount += rowCount;
            List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
            for (int j = 0; j < rowCount; j++) {
                long rowTime = rowStartTime + j;
                if (timeCounter.containsKey(rowTime)) {
                    timeCounter.put(rowTime, timeCounter.get(rowTime) + 1);
                } else {
                    timeCounter.put(rowTime, 1L);
                }
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("tagA", "" + random.nextInt(10));
                tags.put("tagB", "" + random.nextInt(10));
                TimeseriesKey timeseriesKey = new TimeseriesKey(measurement, tags);
                TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey, rowTime);
                for (int f = 0; f < 5; f++) {
                    timeseriesRow.addField("long_" + f, ColumnValue.fromLong(random.nextInt()));
                    timeseriesRow.addField("string_" + f, ColumnValue.fromString("" + random.nextInt()));
                    timeseriesRow.addField("bool_" + f, ColumnValue.fromBoolean(random.nextBoolean()));
                    timeseriesRow.addField("double_" + f, ColumnValue.fromDouble(random.nextDouble()));
                    timeseriesRow.addField("binary_" + f, ColumnValue.fromBinary(("" + random.nextInt()).getBytes()));
                }
                rows.add(timeseriesRow);
                if (rows.size() == 100 || (j == rowCount - 1)) {
                    PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
                    putTimeseriesDataRequest.setRows(rows);
                    PutTimeseriesDataResponse putTimeseriesDataResponse = client.putTimeseriesData(putTimeseriesDataRequest);
                    Assert.assertTrue(putTimeseriesDataResponse.isAllSuccess());
                    rows.clear();
                }
            }
        }
        System.out.println(totalRowCount);
    }

    @Before
    public void setUp() throws Exception {
//        client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(testTable));
//        CreateTimeseriesTableRequest request = new CreateTimeseriesTableRequest(new TimeseriesTableMeta(testTable));
//        client.createTimeseriesTable(request);
//        Thread.sleep(35000);
//        loadData();
    }

    @Test
    public void testSplitTable() {
        SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(testTable);
        try {
            client.splitTimeseriesScanTask(request);
            fail();
        } catch (IllegalArgumentException ex) {
        }
        try {
            request = new SplitTimeseriesScanTaskRequest(testTable, -1);
            client.splitTimeseriesScanTask(request);
            fail();
        } catch (IllegalArgumentException ex) {
        }
        for (int i = 1; i < 500; i++) {
            request = new SplitTimeseriesScanTaskRequest(testTable, i);
            SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
            if (i <= 256) {
                assertEquals(i, response.getSplitInfos().size());
            } else {
                assertEquals(256, response.getSplitInfos().size());
            }
        }
    }

    @Test
    public void testSplitMeasurement() {
        List<String> measurements = new ArrayList<String>(measurementRowCount.keySet());
        String measurement = measurements.get(new Random().nextInt(measurements.size()));
        SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(testTable, measurement);
        try {
            client.splitTimeseriesScanTask(request);
            fail();
        } catch (IllegalArgumentException ex) {
        }
        try {
            request = new SplitTimeseriesScanTaskRequest(testTable, measurement, -1);
            client.splitTimeseriesScanTask(request);
            fail();
        } catch (IllegalArgumentException ex) {
        }
        for (int i = 1; i < 500; i++) {
            request = new SplitTimeseriesScanTaskRequest(testTable, measurement, i);
            SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
            if (i <= 256) {
                assertEquals(i, response.getSplitInfos().size());
            } else {
                assertEquals(256, response.getSplitInfos().size());
            }
        }
    }

    @Test
    public void testSplitTableAndRead() {
        Random random = new Random();
        int testCount = 20;
        for (int i = 0; i < testCount; i++) {
            int split = random.nextInt(256) + 1;
            SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(testTable, split);
            SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
            assertEquals(split, response.getSplitInfos().size());
            long readCount = 0;
            for (int j = 0; j < response.getSplitInfos().size(); j++) {
                ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(testTable);
                scanTimeseriesDataRequest.setSplitInfo(response.getSplitInfos().get(j));
                while (true) {
                    if (random.nextBoolean()) {
                        scanTimeseriesDataRequest.setLimit(100+random.nextInt(3000));
                    }
                    ScanTimeseriesDataResponse scanTimeseriesDataResponse = client.scanTimeseriesData(scanTimeseriesDataRequest);
                    readCount += scanTimeseriesDataResponse.getRows().size();
                    if (scanTimeseriesDataResponse.getNextToken() == null) {
                        break;
                    }
                    scanTimeseriesDataRequest.setNextToken(scanTimeseriesDataResponse.getNextToken());
                }
            }
            assertEquals(totalRowCount, readCount);
        }
    }

    @Test
    public void testSplitMeasurementAndRead() {
        Random random = new Random();
        List<String> measurements = new ArrayList<String>(measurementRowCount.keySet());
        int testCount = 20;
        for (int i = 0; i < testCount; i++) {
            int split = random.nextInt(256) + 1;
            String measurement = measurements.get(new Random().nextInt(measurements.size()));
            SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(testTable, measurement, split);
            SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
            assertEquals(split, response.getSplitInfos().size());
            long readCount = 0;
            for (int j = 0; j < response.getSplitInfos().size(); j++) {
                ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(testTable);
                scanTimeseriesDataRequest.setSplitInfo(response.getSplitInfos().get(j));
                while (true) {
                    if (random.nextBoolean()) {
                        scanTimeseriesDataRequest.setLimit(10+random.nextInt(100));
                    }
                    ScanTimeseriesDataResponse scanTimeseriesDataResponse = client.scanTimeseriesData(scanTimeseriesDataRequest);
                    readCount += scanTimeseriesDataResponse.getRows().size();
                    if (scanTimeseriesDataResponse.getNextToken() == null) {
                        break;
                    }
                    scanTimeseriesDataRequest.setNextToken(scanTimeseriesDataResponse.getNextToken());
                }
            }
            assertEquals(measurementRowCount.get(measurement).longValue(), readCount);
        }
    }

    @Test
    public void testScanWithTimeRangeAndFieldsToGet() {
        Random random = new Random();
        int testCount = 20;
        for (int i = 0; i < testCount; i++) {
            long minTime = timeCounter.firstEntry().getKey();
            long maxTime = timeCounter.lastEntry().getKey();
            long startTime = minTime + random.nextInt((int) ((maxTime - minTime) / 2));
            long endTime = startTime + 1 + random.nextInt((int) (maxTime - startTime));
            long rowCountInTimeRange = 0;
            for (long j = startTime; j < endTime; j++) {
                rowCountInTimeRange += timeCounter.get(j);
            }
            Map<String, ColumnType> fieldsToGet = new HashMap<String, ColumnType>();
            for (int j = 0; j < 5; j++) {
                if (random.nextBoolean()) {
                    fieldsToGet.put("string_" + j, ColumnType.STRING);
                }
                if (random.nextBoolean()) {
                    fieldsToGet.put("long_" + j, ColumnType.INTEGER);
                }
                if (random.nextBoolean()) {
                    fieldsToGet.put("double_" + j, ColumnType.DOUBLE);
                }
                if (random.nextBoolean()) {
                    fieldsToGet.put("bool_" + j, ColumnType.BOOLEAN);
                }
                if (random.nextBoolean()) {
                    fieldsToGet.put("binary_" + j, ColumnType.BINARY);
                }
            }

            int split = random.nextInt(256) + 1;
            SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(testTable, split);
            SplitTimeseriesScanTaskResponse response = client.splitTimeseriesScanTask(request);
            assertEquals(split, response.getSplitInfos().size());
            long readCount = 0;
            for (int j = 0; j < response.getSplitInfos().size(); j++) {
                ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(testTable);
                scanTimeseriesDataRequest.setSplitInfo(response.getSplitInfos().get(j));
                scanTimeseriesDataRequest.setTimeRange(startTime, endTime);
                for (String field : fieldsToGet.keySet()) {
                    scanTimeseriesDataRequest.addFieldToGet(field, fieldsToGet.get(field));
                }
                while (true) {
                    if (random.nextBoolean()) {
                        scanTimeseriesDataRequest.setLimit(100+random.nextInt(1000));
                    }
                    ScanTimeseriesDataResponse scanTimeseriesDataResponse = client.scanTimeseriesData(scanTimeseriesDataRequest);
                    readCount += scanTimeseriesDataResponse.getRows().size();
                    for (TimeseriesRow row : scanTimeseriesDataResponse.getRows()) {
                        assertEquals(fieldsToGet.isEmpty() ? 25 : fieldsToGet.size(), row.getFields().size());
                        if (!fieldsToGet.isEmpty()) {
                            for (String field : row.getFields().keySet()) {
                                assertTrue(fieldsToGet.containsKey(field));
                            }
                        }
                        assertTrue(row.getTimeInUs() >= startTime);
                        assertTrue(row.getTimeInUs() < endTime);
                    }
                    if (scanTimeseriesDataResponse.getNextToken() == null) {
                        break;
                    }
                    scanTimeseriesDataRequest.setNextToken(scanTimeseriesDataResponse.getNextToken());
                }
            }
            assertEquals(rowCountInTimeRange, readCount);
        }
    }

}
