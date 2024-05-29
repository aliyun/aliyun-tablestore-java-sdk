package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.*;

public class TimeseriesLastpointIndexTest {

    static SyncClient client = null;
    static TimeseriesClient timeseriesClient = null;
    static long baseTimeMs = System.currentTimeMillis();

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setRetryStrategy(new AlwaysRetryStrategy(
                200, 10, 1000));
        client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        timeseriesClient = client.asTimeseriesClient();
    }

    @AfterClass
    public static void afterClass() {
        client.shutdown();
    }

    public static void sleepSecond(long second) {
        try {
            Thread.sleep(second * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class TimeseriesDataProducer {
        static long LOW_BITS = 24;
        static long MAX_COLUMN_COUNT = 20;
        static long MOD = 99791;
        static Random random = new Random();

        public boolean fixedColumnType = false;

        public TimeseriesKey buildTsKey(long keyId) {
            return new TimeseriesKey("test", "key_" + keyId);
        }

        public long getKeyId(long rowId) {
            return rowId >> LOW_BITS;
        }

        public long getTimeUs(long rowId) {
            long rowIdLow = rowId & ((1L << LOW_BITS) - 1);
            return (baseTimeMs + rowIdLow) * 1000;
        }

        public long getColumnCount(long rowId) {
            if (fixedColumnType) {
                return 10;
            }
            return (rowId % MAX_COLUMN_COUNT) + 1;
        }

        public String getColumnName(long columnId) {
            return "col_" + columnId;
        }

        public ColumnValue getColumnValue(long rowId, long columnId) {
            if (columnId == 0) {
                return ColumnValue.fromLong(rowId);
            }
            long value = rowId % MOD;
            if (fixedColumnType) {
                if (columnId % 2 == 1) {
                    return ColumnValue.fromLong(value);
                } else {
                    return ColumnValue.fromString("value_" + value);
                }
            } else {
                if ((rowId + columnId) % 2 == 0) {
                    return ColumnValue.fromLong(value);
                } else {
                    return ColumnValue.fromString("value_" + value);
                }
            }
        }

        public TimeseriesRow buildRow(long rowId) {
            TimeseriesKey timeseriesKey = buildTsKey(getKeyId(rowId));
            TimeseriesRow timeseriesRow = new TimeseriesRow(timeseriesKey);
            timeseriesRow.setTimeInUs(getTimeUs(rowId));
            long columnCount = getColumnCount(rowId);
            for (long i = 0; i < columnCount; i++) {
                timeseriesRow.addField(getColumnName(i), getColumnValue(rowId, i));
            }
            return timeseriesRow;
        }

        public long getRowIdByRow(TimeseriesRow row) {
            String dataSource = row.getTimeseriesKey().getDataSource();
            long keyId = Long.parseLong(dataSource.substring(4));
            long rowId = row.getFields().get("col_0").asLong();
            if (getKeyId(rowId) != keyId) {
                throw new RuntimeException("keyId not match");
            }
            return rowId;
        }

        public long getKeyIdByRow(Row row) {
            String dataSource = row.getPrimaryKey().getPrimaryKeyColumn("_data_source").getValue().asString();
            long keyId = Long.parseLong(dataSource.substring(4));
            return keyId;
        }

        public Set<Long> generateRandomKeyIds(int n) {
            Set<Long> keyIds = new HashSet<Long>();
            while (keyIds.size() < n) {
                long keyId = random.nextInt(100 * n);
                keyIds.add(keyId);
            }
            return keyIds;
        }

        public List<Long> generateRandomRowIds(long keyId, int n) {
            List<Long> rowIds = new ArrayList<Long>();
            while (rowIds.size() < n) {
                long rowIdLow = random.nextInt(100 * n);
                long rowId = (keyId << LOW_BITS) | rowIdLow;
                rowIds.add(rowId);
            }
            return rowIds;
        }

        public Map<Long, List<Long>> selectRandomData(
                int maxTimelines,
                int maxRowsPerTimeline) {
            Map<Long, List<Long>> result = new HashMap<Long, List<Long>>();
            Set<Long> keyIds = generateRandomKeyIds(maxTimelines);
            for (Long keyId : keyIds) {
                List<Long> rowIds = generateRandomRowIds(keyId, 1 + random.nextInt(maxRowsPerTimeline));
                result.put(keyId, rowIds);
            }
            return result;
        }

        public Row buildLastpointRow(String hashKey, long keyId, List<Long> rowIds) {
            TimeseriesKey timeseriesKey = buildTsKey(keyId);
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("_#h", PrimaryKeyValue.fromString(hashKey))
                    .addPrimaryKeyColumn("_m_name", PrimaryKeyValue.fromString(timeseriesKey.getMeasurementName()))
                    .addPrimaryKeyColumn("_data_source", PrimaryKeyValue.fromString(timeseriesKey.getDataSource()))
                    .addPrimaryKeyColumn("_tags", PrimaryKeyValue.fromString("[]"))
                    .build();
            Map<String, Column> columnsMap = new TreeMap<String, Column>();
            long maxTimeUs = 0;
            for (long rowId : rowIds) {
                long timeUs = getTimeUs(rowId);
                long columnCount = getColumnCount(rowId);
                for (long i = 0; i < columnCount; i++) {
                    String columnName = getColumnName(i);
                    ColumnValue columnValue = getColumnValue(rowId, i);
                    if (!columnsMap.containsKey(columnName)) {
                        columnsMap.put(columnName, new Column(columnName, columnValue, timeUs/1000));
                    } else if (columnsMap.get(columnName).getTimestamp() < timeUs/1000) {
                        columnsMap.put(columnName, new Column(columnName, columnValue, timeUs/1000));
                    }
                }
                if (timeUs > maxTimeUs) {
                    maxTimeUs = timeUs;
                }
            }
            columnsMap.put("_time", new Column("_time", ColumnValue.fromLong(maxTimeUs), maxTimeUs/1000));
            return new Row(primaryKey, columnsMap.values().toArray(new Column[0]));
        }

        public StreamRecord buildLastpointIndexStreamRecord(String hashKey, long rowId) {
            TimeseriesRow timeseriesRow = buildRow(rowId);
            StreamRecord streamRecord = new StreamRecord();
            streamRecord.setRecordType(StreamRecord.RecordType.UPDATE);
            TimeseriesKey timeseriesKey = timeseriesRow.getTimeseriesKey();
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("_#h", PrimaryKeyValue.fromString(hashKey))
                    .addPrimaryKeyColumn("_m_name", PrimaryKeyValue.fromString(timeseriesKey.getMeasurementName()))
                    .addPrimaryKeyColumn("_data_source", PrimaryKeyValue.fromString(timeseriesKey.getDataSource()))
                    .addPrimaryKeyColumn("_tags", PrimaryKeyValue.fromString("[]"))
                    .build();
            streamRecord.setPrimaryKey(primaryKey);
            List<RecordColumn> recordColumns = new ArrayList<RecordColumn>();
            for (Map.Entry<String, ColumnValue> entry : timeseriesRow.getFields().entrySet()) {
                Column column = new Column(entry.getKey(), entry.getValue(), timeseriesRow.getTimeInUs() / 1000);
                recordColumns.add(new RecordColumn(column, RecordColumn.ColumnType.PUT));
            }
            recordColumns.add(new RecordColumn(
                    new Column("_time", ColumnValue.fromLong(timeseriesRow.getTimeInUs()), timeseriesRow.getTimeInUs()/ 1000),
                    RecordColumn.ColumnType.PUT));
            Collections.sort(recordColumns, new Comparator<RecordColumn>() {
                        @Override
                        public int compare(RecordColumn o1, RecordColumn o2) {
                            return o1.getColumn().getName().compareTo(o2.getColumn().getName());
                        }
                    });
            streamRecord.setColumns(recordColumns);
            return streamRecord;
        }

        public List<Long> shuffleData(Map<Long, List<Long>> data) {
            List<Long> allRowIds = new ArrayList<Long>();
            for (Map.Entry<Long, List<Long>> entry : data.entrySet()) {
                allRowIds.addAll(entry.getValue());
            }
            Collections.shuffle(allRowIds);
            return allRowIds;
        }

        public void writeRows(String tableName, Map<Long, List<Long>> data) {
            List<Long> allRowIds = shuffleData(data);
            writeRows(tableName, allRowIds);
        }

        public void writeRows(String tableName, List<Long> allRowIds) {
            int batchSize = 1 + random.nextInt(200);
            if (random.nextBoolean()) {
                // test small batch
                batchSize = 1 + random.nextInt(4);
            }
            for (int i = 0; i < allRowIds.size(); i += batchSize) {
                List<Long> rowIds = allRowIds.subList(i, Math.min(i + batchSize, allRowIds.size()));
                List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
                for (Long rowId : rowIds) {
                    rows.add(buildRow(rowId));
                }
                PutTimeseriesDataRequest request = new PutTimeseriesDataRequest(tableName);
                request.setRows(rows);
                PutTimeseriesDataResponse putTimeseriesDataResponse = timeseriesClient.putTimeseriesData(request);
                if (!putTimeseriesDataResponse.isAllSuccess()) {
                    throw new RuntimeException("putTimeseriesData failed, failed count:" +
                            putTimeseriesDataResponse.getFailedRows().size() + ", error: " +
                            putTimeseriesDataResponse.getFailedRows().get(0).getError().toString());
                }
            }
        }
    }

    private boolean timeseriesTableExist(String tableName) {
        ListTimeseriesTableResponse listTimeseriesTableResponse = timeseriesClient.listTimeseriesTable();
        for (String name : listTimeseriesTableResponse.getTimeseriesTableNames()) {
            if (name.equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private void createTimeseriesTable(String tableName, boolean deleteIfAlreadyExist) {
        createTimeseriesTableWithLastpointIndex(tableName, "", deleteIfAlreadyExist);
    }

    private void createTimeseriesTableWithLastpointIndex(
            String tableName, String lastpointIndexName, boolean deleteIfAlreadyExist) {
        if (deleteIfAlreadyExist && timeseriesTableExist(tableName)) {
            DeleteTimeseriesTableRequest request = new DeleteTimeseriesTableRequest(tableName);
            timeseriesClient.deleteTimeseriesTable(request);
        }
        CreateTimeseriesTableRequest request =
                new CreateTimeseriesTableRequest(new TimeseriesTableMeta(tableName));
        request.setEnableAnalyticalStore(true);
        if (!lastpointIndexName.isEmpty()) {
            request.addLastpointIndex(new CreateTimeseriesTableRequest.LastpointIndex(lastpointIndexName));
        }
        timeseriesClient.createTimeseriesTable(request);
    }

    private void createTimeseriesLastpointIndex(String tableName, String indexName, boolean includeBaseData) {
        CreateTimeseriesLastpointIndexRequest request =
                new CreateTimeseriesLastpointIndexRequest(tableName, indexName, includeBaseData);
        timeseriesClient.createTimeseriesLastpointIndex(request);
    }

    private void deleteTimeseriesLastpointIndex(String tableName, String indexName) {
        DeleteTimeseriesLastpointIndexRequest request =
                new DeleteTimeseriesLastpointIndexRequest(tableName, indexName);
        timeseriesClient.deleteTimeseriesLastpointIndex(request);
    }

    private List<TimeseriesRow> readTimeseriesTable(String tableName) {
        SplitTimeseriesScanTaskRequest request = new SplitTimeseriesScanTaskRequest(tableName, 1);
        SplitTimeseriesScanTaskResponse response = timeseriesClient.splitTimeseriesScanTask(request);
        long readCount = 0;
        List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
        for (int j = 0; j < response.getSplitInfos().size(); j++) {
            ScanTimeseriesDataRequest scanTimeseriesDataRequest = new ScanTimeseriesDataRequest(tableName);
            scanTimeseriesDataRequest.setSplitInfo(response.getSplitInfos().get(j));
            while (true) {
                ScanTimeseriesDataResponse scanTimeseriesDataResponse = timeseriesClient.scanTimeseriesData(scanTimeseriesDataRequest);
                readCount += scanTimeseriesDataResponse.getRows().size();
                rows.addAll(scanTimeseriesDataResponse.getRows());
                if (scanTimeseriesDataResponse.getNextToken() == null) {
                    break;
                }
                scanTimeseriesDataRequest.setNextToken(scanTimeseriesDataResponse.getNextToken());
            }
        }
        System.out.println("readCount:" + readCount);
        return rows;
    }

    private List<Row> readLastpointIndexTable(String indexTableName) {
        DescribeTableResponse describeTableResponse = describeLastpointIndex(indexTableName);
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(indexTableName);
        PrimaryKeyBuilder beginKey = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeySchema pk : describeTableResponse.getTableMeta().getPrimaryKeyList()) {
            beginKey.addPrimaryKeyColumn(pk.getName(), PrimaryKeyValue.INF_MIN);
        }
        rangeIteratorParameter.setInclusiveStartPrimaryKey(beginKey.build());
        PrimaryKeyBuilder endKey = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeySchema pk : describeTableResponse.getTableMeta().getPrimaryKeyList()) {
            endKey.addPrimaryKeyColumn(pk.getName(), PrimaryKeyValue.INF_MAX);
        }
        rangeIteratorParameter.setExclusiveEndPrimaryKey(endKey.build());
        rangeIteratorParameter.setMaxVersions(1);

        Iterator<Row> rowIter = client.createRangeIterator(rangeIteratorParameter);
        List<Row> rows = new ArrayList<Row>();
        while (rowIter.hasNext()) {
            rows.add(rowIter.next());
        }
        return rows;
    }

    private DescribeTableResponse describeLastpointIndex(String tableName) {
        return client.describeTable(new DescribeTableRequest(tableName));
    }

    private DescribeTimeseriesTableResponse describeTimeseriesTable(String tableName) {
        return timeseriesClient.describeTimeseriesTable(new DescribeTimeseriesTableRequest(tableName));
    }

    private String getStreamId(String tableName) {
        ListStreamRequest listStreamRequest = new ListStreamRequest();
        listStreamRequest.setTableName(tableName);
        ListStreamResponse listStreamResponse = client.listStream(listStreamRequest);
        String streamId = listStreamResponse.getStreams().get(0).getStreamId();
        return streamId;
    }

    private List<StreamRecord> readAllStreamRows(String tableName, String streamId) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(streamId);
        describeStreamRequest.setSupportTimeseriesTable(true);
        DescribeStreamResponse describeStreamResponse = client.describeStream(describeStreamRequest);
        assertEquals(describeStreamResponse.getNextShardId(), null);
        List<StreamShard> shards = describeStreamResponse.getShards();
        List<StreamRecord> result = new ArrayList<StreamRecord>();
        for (StreamShard shard : shards) {
            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest(streamId, shard.getShardId());
            GetShardIteratorResponse getShardIteratorResponse = client.getShardIterator(getShardIteratorRequest);
            GetStreamRecordRequest getStreamRecordRequest =
                    new GetStreamRecordRequest(getShardIteratorResponse.getShardIterator());
            while (true) {
                getStreamRecordRequest.setTableName(tableName);
                GetStreamRecordResponse getStreamRecordResponse = client.getStreamRecord(getStreamRecordRequest);
                result.addAll(getStreamRecordResponse.getRecords());
                if (getStreamRecordResponse.getRecords().size() == 0 && !getStreamRecordResponse.getMayMoreRecord()) {
                    break;
                }
                getStreamRecordRequest.setShardIterator(getStreamRecordResponse.getNextShardIterator());
            }
        }
        return result;
    }

    private void readAndCheckMainTableRows(
            String tableName,
            TimeseriesDataProducer producer,
            Map<Long, List<Long>> data) {
        // 读取主表数据
        List<TimeseriesRow> rows = readTimeseriesTable(tableName);
        // 验证
        Set<Long> allRowIds = new HashSet<Long>();
        for (Map.Entry<Long, List<Long>> entry : data.entrySet()) {
            allRowIds.addAll(entry.getValue());
        }
        assertEquals(allRowIds.size(), rows.size());

        for (TimeseriesRow row : rows) {
            long rowId = producer.getRowIdByRow(row);
            if (!allRowIds.contains(rowId)) {
                throw new RuntimeException("rowId:" + rowId + " not found");
            }
            TimeseriesRow expectRow = producer.buildRow(rowId);
            assertEquals(expectRow, row);
        }
    }

    private void checkLastpointIndexRows(
            TimeseriesDataProducer producer,
            Map<Long, List<Long>> data,
            List<Row> indexRows) {
        assertEquals(data.size(), indexRows.size());
        for (Row row : indexRows) {
            long keyId = producer.getKeyIdByRow(row);
            if (!data.containsKey(keyId)) {
                throw new RuntimeException("keyId:" + keyId + " not found");
            }
            Row expect = producer.buildLastpointRow(
                    row.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString(),
                    keyId,
                    data.get(keyId));
            assertEquals(expect.toString(), row.toString());
        }
    }

    private void readAndCheckLastpointIndexRows(
            String indexTableName,
            TimeseriesDataProducer producer,
            Map<Long, List<Long>> data) {
        // 读取lastpoint索引数据
        List<Row> indexRows = readLastpointIndexTable(indexTableName);

        // 验证
        checkLastpointIndexRows(producer, data, indexRows);
    }

    @Test
    public void testTimeseriesLastpointIndexCreateAndDelete() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";

        // 创建时序表
        createTimeseriesTable(tableName, true);
        // 创建时序表的lastpoint索引
        createTimeseriesLastpointIndex(tableName, indexName, true);

        // 查询时序表信息
        DescribeTimeseriesTableResponse describeTimeseriesTableResponse =
                describeTimeseriesTable(tableName);
        assertEquals(1, describeTimeseriesTableResponse.getLastpointIndexes().size());
        assertEquals(indexName, describeTimeseriesTableResponse.getLastpointIndexes().get(0)
                .getLastpointIndexName());

        // 查询时序表的lastpoint索引信息
        DescribeTableResponse response = describeLastpointIndex(indexName);
        assertEquals(indexName, response.getTableMeta().getTableName());
        assertEquals(4,    response.getTableMeta().getPrimaryKeyList().size());
        assertEquals("_#h", response.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals("_m_name", response.getTableMeta().getPrimaryKeyList().get(1).getName());
        assertEquals("_data_source", response.getTableMeta().getPrimaryKeyList().get(2).getName());
        assertEquals("_tags", response.getTableMeta().getPrimaryKeyList().get(3).getName());

        // 删除时序表的lastpoint索引
        deleteTimeseriesLastpointIndex(tableName, indexName);
        // 删除时序表
        DeleteTimeseriesTableRequest request = new DeleteTimeseriesTableRequest(tableName);
        timeseriesClient.deleteTimeseriesTable(request);
    }

    @Test
    public void testCreateTimeseriesTableWithLastpointIndex() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";

        createTimeseriesTableWithLastpointIndex(tableName, indexName, true);
        // 查询时序表信息
        DescribeTimeseriesTableResponse describeTimeseriesTableResponse =
                describeTimeseriesTable(tableName);
        assertEquals(1, describeTimeseriesTableResponse.getLastpointIndexes().size());
        assertEquals(indexName, describeTimeseriesTableResponse.getLastpointIndexes().get(0)
                .getLastpointIndexName());

        // 查询时序表的lastpoint索引信息
        DescribeTableResponse response = describeLastpointIndex(indexName);
        assertEquals(indexName, response.getTableMeta().getTableName());
        assertEquals(4,    response.getTableMeta().getPrimaryKeyList().size());
        assertEquals("_#h", response.getTableMeta().getPrimaryKeyList().get(0).getName());
        assertEquals("_m_name", response.getTableMeta().getPrimaryKeyList().get(1).getName());
        assertEquals("_data_source", response.getTableMeta().getPrimaryKeyList().get(2).getName());
        assertEquals("_tags", response.getTableMeta().getPrimaryKeyList().get(3).getName());

        boolean random = Math.random() < 0.5;
        if (random) {
            // 删除时序表的lastpoint索引
            deleteTimeseriesLastpointIndex(tableName, indexName);
        }
        // 删除时序表
        DeleteTimeseriesTableRequest request = new DeleteTimeseriesTableRequest(tableName);
        timeseriesClient.deleteTimeseriesTable(request);
    }

    @Test
    public void testTimeseriesLastpointIndexWithIncrData() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";

        boolean createIndexAfterCreateTable = Math.random() < 0.5;
        System.out.println("createIndexAfterCreateTable:" + createIndexAfterCreateTable);
        if (createIndexAfterCreateTable) {
            // 创建时序表
            createTimeseriesTable(tableName, true);
            // 创建时序表的lastpoint索引
            createTimeseriesLastpointIndex(tableName, indexName, false);
        } else {
            // 创建时序表时同时创建时序表的lastpoint索引
            createTimeseriesTableWithLastpointIndex(tableName, indexName, true);
        }
        sleepSecond(35);

        // 写入数据
        TimeseriesDataProducer producer = new TimeseriesDataProducer();
        Map<Long, List<Long>> data = producer.selectRandomData(100, 100);
        producer.writeRows(tableName, data);

        readAndCheckMainTableRows(tableName, producer, data);
        readAndCheckLastpointIndexRows(indexName, producer, data);
    }

    @Test
    public void testTimeseriesLastpointIndexWithBaseData() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";
        // 创建时序表
        createTimeseriesTable(tableName, true);
        sleepSecond(35);

        // 写入数据
        TimeseriesDataProducer producer = new TimeseriesDataProducer();
        Map<Long, List<Long>> data = producer.selectRandomData(100, 100);
        producer.writeRows(tableName, data);

        // 验证
        readAndCheckMainTableRows(tableName, producer, data);

        // 创建时序表的lastpoint索引
        createTimeseriesLastpointIndex(tableName, indexName, true);

        // 读取lastpoint索引数据
        sleepSecond(60);
        List<Row> indexRows;
        long startTime = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - startTime > 300 * 1000) {
                fail("read lastpoint index table timeout");
            }
            try {
                indexRows = readLastpointIndexTable(indexName);
                break;
            } catch (TableStoreException ex) {
                if (ex.getMessage().contains("Disallow read local index")) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw ex;
                }
            }
        }

        // 验证
        checkLastpointIndexRows(producer, data, indexRows);
    }

    @Test
    public void testTimeseriesLastpointIndexStream() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";
        // 创建时序表
        createTimeseriesTable(tableName, true);
        // 创建时序表的lastpoint索引
        createTimeseriesLastpointIndex(tableName, indexName, false);
        sleepSecond(35);

        // 写入数据
        TimeseriesDataProducer producer = new TimeseriesDataProducer();
        Map<Long, List<Long>> data = producer.selectRandomData(10, 10);
        List<Long> rowIds = producer.shuffleData(data);

        producer.writeRows(tableName, rowIds);
        readAndCheckMainTableRows(tableName, producer, data);

        String streamId = getStreamId(tableName);
        List<StreamRecord> streamRecords = readAllStreamRows(indexName, streamId);
        assertEquals(streamRecords.size(), rowIds.size());
        Set<Long> rowIdSet = new HashSet<Long>(rowIds);

        for (StreamRecord record : streamRecords) {
            RecordColumn col0 = record.getColumns().get(1);
            assertEquals("col_0", col0.getColumn().getName());
            long rowId = col0.getColumn().getValue().asLong();
            assertTrue(rowIdSet.contains(rowId));
            String hashKey = record.getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString();
            StreamRecord expect = producer.buildLastpointIndexStreamRecord(hashKey, rowId);
            assertEquals(expect.getRecordType(), record.getRecordType());
            assertEquals(expect.getPrimaryKey(), record.getPrimaryKey());
            assertEquals(expect.getColumns().toString(), record.getColumns().toString());
        }
    }

    @Test
    public void testTimeseriesLastpointIndexCreateSearchIndexAndSQL() {
        String methodName = Thread.currentThread().getStackTrace()[1].getMethodName();
        String tableName = methodName;
        String indexName = methodName + "_lastpoint_index";
        // 创建时序表
        createTimeseriesTable(tableName, true);
        // 创建时序表的lastpoint索引
        createTimeseriesLastpointIndex(tableName, indexName, false);
        sleepSecond(35);

        // 写入数据
        TimeseriesDataProducer producer = new TimeseriesDataProducer();
        producer.fixedColumnType = true;
        Map<Long, List<Long>> data = producer.selectRandomData(100, 100);
        producer.writeRows(tableName, data);

        readAndCheckMainTableRows(tableName, producer, data);
        List<Row> indexRows = readLastpointIndexTable(indexName);
        checkLastpointIndexRows(producer, data, indexRows);

        // 创建lastpoint index的多元索引
        CreateSearchIndexRequest createSearchIndexRequest =
                new CreateSearchIndexRequest(indexName, indexName + "_search");
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.addFieldSchema(new FieldSchema("_m_name", FieldType.KEYWORD));
        indexSchema.addFieldSchema(new FieldSchema("_data_source", FieldType.KEYWORD));
        indexSchema.addFieldSchema(new FieldSchema("_tags", FieldType.KEYWORD));
        indexSchema.addFieldSchema(new FieldSchema("_time", FieldType.LONG));
        indexSchema.addFieldSchema(new FieldSchema("col_1", FieldType.LONG));
        indexSchema.addFieldSchema(new FieldSchema("col_2", FieldType.KEYWORD));
        indexSchema.addFieldSchema(new FieldSchema("col_3", FieldType.LONG));
        indexSchema.addFieldSchema(new FieldSchema("col_4", FieldType.KEYWORD));
        createSearchIndexRequest.setIndexSchema(indexSchema);
        client.createSearchIndex(createSearchIndexRequest);

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(new MatchAllQuery());
        searchQuery.setLimit(100);
        searchQuery.setGetTotalCount(true);
        SearchRequest searchRequest = new SearchRequest(indexName, indexName + "_search", searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setColumns(Arrays.asList("col_1", "col_2", "col_3", "col_4"));
        searchRequest.setColumnsToGet(columnsToGet);
        long startTime = System.currentTimeMillis();
        while (true) {
            SearchResponse searchResponse = client.search(searchRequest);
            if (searchResponse.getTotalCount() == indexRows.size()) {
                for (Row row : searchResponse.getRows()) {
                    assertTrue(row.getLatestColumn("col_1").getValue().asLong() > 0);
                    assertTrue(!row.getLatestColumn("col_2").getValue().asString().isEmpty());
                    assertTrue(row.getLatestColumn("col_3").getValue().asLong() > 0);
                    assertTrue(!row.getLatestColumn("col_4").getValue().asString().isEmpty());
                }
                break;
            }
            if (System.currentTimeMillis() - startTime > 180000) {
                fail("wait search index ready timeout");
            }
            sleepSecond(5);
        }

        // 创建sql绑定
        String createSqlBindingStmt = String.format("CREATE TABLE %s (" +
                "`_#h` varchar(1024) NOT NULL," +
                "`_m_name` varchar(1024) NOT NULL," +
                "`_data_source` varchar(1024) NOT NULL," +
                "`_tags` varchar(1024) NOT NULL," +
                "`col_1` bigint(20) NOT NULL," +
                "`col_2` mediumtext NOT NULL," +
                "`col_3` bigint(20) NOT NULL," +
                "`col_4` mediumtext NOT NULL," +
                "PRIMARY KEY (`_#h`, `_m_name`,`_data_source`,`_tags`)" +
                ")", indexName);
        try {
            client.sqlQuery(new SQLQueryRequest(
                    "drop mapping table " + indexName + ";"));
        } catch (TableStoreException ex) {
            // ignore
        }
        SQLQueryRequest sqlQueryRequest = new SQLQueryRequest(createSqlBindingStmt);
        client.sqlQuery(sqlQueryRequest);

        String queryStmt = String.format("select count(*) from %s where col_1 > 0", indexName);
        sqlQueryRequest = new SQLQueryRequest(queryStmt);
        SQLQueryResponse sqlQueryResponse = client.sqlQuery(sqlQueryRequest);
        long countValue = sqlQueryResponse.getSQLResultSet().next().getLong(0);
        assertEquals(indexRows.size(), countValue);
    }
}
