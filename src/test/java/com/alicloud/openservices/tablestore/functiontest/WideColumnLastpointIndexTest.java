package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesLastpointIndexRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesLastpointIndexRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class WideColumnLastpointIndexTest {

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

    private boolean tableExist(String tableName) {
        ListTableResponse listTableResponse = client.listTable();
        for (String name : listTableResponse.getTableNames()) {
            if (name.equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    private void createTable(String tableName, boolean deleteIfAlreadyExist) {
        if (deleteIfAlreadyExist && tableExist(tableName)) {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            client.deleteTable(request);
        }
        TableOptions tableOptions = new TableOptions(86400*30, 1);
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk4", PrimaryKeyType.INTEGER);
        tableOptions.setAllowUpdate(false);
        CreateTableRequest createTableRequest =
                new CreateTableRequest(tableMeta, tableOptions);
        client.createTable(createTableRequest);
    }

    @Test
    public void testCreateAndDeleteLastpointIndex() {
        String tableName = getClass().getSimpleName() + "_" + "testCreateAndDeleteLastpointIndex";
        String indexName = tableName + "_index";
        createTable(tableName, true);
        CreateTimeseriesLastpointIndexRequest createTimeseriesLastpointIndexRequest =
                new CreateTimeseriesLastpointIndexRequest(
                        tableName, indexName, true);
        createTimeseriesLastpointIndexRequest.setCreateOnWideColumnTable(true);
        createTimeseriesLastpointIndexRequest.setLastpointIndexPrimaryKeyNames(
                Arrays.asList("pk1", "pk2", "pk3"));
        timeseriesClient.createTimeseriesLastpointIndex(createTimeseriesLastpointIndexRequest);
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(indexName);
        DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);
        assertEquals(describeTableResponse.getTableMeta().getTableName(), indexName);
        List<PrimaryKeySchema> expectedPrimaryKeyList = Arrays.asList(
                new PrimaryKeySchema("pk1", PrimaryKeyType.STRING),
                new PrimaryKeySchema("pk2", PrimaryKeyType.INTEGER),
                new PrimaryKeySchema("pk3", PrimaryKeyType.STRING));
        assertEquals(expectedPrimaryKeyList, describeTableResponse.getTableMeta().getPrimaryKeyList());
        assertEquals(describeTableResponse.getTableOptions().getTimeToLive(), 86400*30);
        assertEquals(describeTableResponse.getTableOptions().getMaxVersions(), 1);
        assertEquals(describeTableResponse.getStreamDetails().isEnableStream(), false);

        // update main table stream
        UpdateTableRequest updateTableRequest = new UpdateTableRequest(tableName);
        updateTableRequest.setStreamSpecification(new StreamSpecification(true, 24));
        client.updateTable(updateTableRequest);
        // check index table's stream is on
        describeTableResponse = client.describeTable(describeTableRequest);
        assertEquals(describeTableResponse.getStreamDetails().isEnableStream(), true);

        timeseriesClient.deleteTimeseriesLastpointIndex(new DeleteTimeseriesLastpointIndexRequest(
                tableName, indexName));
    }

    private void writeData(String tableName) {
        for (int i = 0; i < 100; i++) {
            RowPutChange rowPutChange = new RowPutChange(tableName);
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("pk1"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(1000))
                    .addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("pk3_" + i % 10))
                    .addPrimaryKeyColumn("pk4", PrimaryKeyValue.fromLong(i))
                    .build();
            rowPutChange.setPrimaryKey(primaryKey);
            if (i % 10 != 0) {
                rowPutChange.addColumn(new Column("col1", ColumnValue.fromString("col1")));
                rowPutChange.addColumn(new Column("col2:i", ColumnValue.fromLong(100 + i)));
                rowPutChange.addColumn(new Column("col3:s", ColumnValue.fromString("col3")));
            }
            PutRowRequest putRowRequest = new PutRowRequest(rowPutChange);
            client.putRow(putRowRequest);
        }
    }

    private List<Row> readLastpointIndexTable(String indexTableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(indexTableName);
        DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);
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

    @Test
    public void testLastpointIndexIncrData() {
        String tableName = getClass().getSimpleName() + "_" + "testLastpointIndexIncrData";
        String indexName = tableName + "_index";

        createTable(tableName, true);
        CreateTimeseriesLastpointIndexRequest createTimeseriesLastpointIndexRequest =
                new CreateTimeseriesLastpointIndexRequest(
                        tableName, indexName, false);
        createTimeseriesLastpointIndexRequest.setCreateOnWideColumnTable(true);
        createTimeseriesLastpointIndexRequest.setLastpointIndexPrimaryKeyNames(
                Arrays.asList("pk1", "pk2", "pk3"));
        timeseriesClient.createTimeseriesLastpointIndex(createTimeseriesLastpointIndexRequest);
        sleepSecond(10);

        writeData(tableName);

        List<Row> rows = readLastpointIndexTable(indexName);
        assertEquals(9, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(primaryKey.getPrimaryKeyColumn("pk1").getValue().asString(), "pk1");
            assertEquals(primaryKey.getPrimaryKeyColumn("pk2").getValue().asLong(), 1000);
            assertEquals(primaryKey.getPrimaryKeyColumn("pk3").getValue().asString(), "pk3_" + (i+1));
            assertEquals(4, row.getColumns().length);
            assertEquals(row.getColumns()[0].getName(), "col1");
            assertEquals(row.getColumns()[0].getValue().asString(), "col1");
            assertEquals(row.getColumns()[1].getName(), "col2:i");
            assertEquals(row.getColumns()[1].getValue().asLong(), 191 + i);
            assertEquals(row.getColumns()[2].getName(), "col3:s");
            assertEquals(row.getColumns()[2].getValue().asString(), "col3");
            assertEquals(row.getColumns()[3].getName(), "pk4");
            assertEquals(row.getColumns()[3].getValue().asLong(), 91+i);
        }
    }

    @Ignore
    @Test
    public void testLastpointIndexBaseData() {
        String tableName = getClass().getSimpleName() + "_" + "testLastpointIndexBaseData";
        String indexName = tableName + "_index";

        createTable(tableName, true);
        sleepSecond(10);

        writeData(tableName);

        CreateTimeseriesLastpointIndexRequest createTimeseriesLastpointIndexRequest =
                new CreateTimeseriesLastpointIndexRequest(
                        tableName, indexName, true);
        createTimeseriesLastpointIndexRequest.setCreateOnWideColumnTable(true);
        createTimeseriesLastpointIndexRequest.setLastpointIndexPrimaryKeyNames(
                Arrays.asList("pk1", "pk2", "pk3"));
        timeseriesClient.createTimeseriesLastpointIndex(createTimeseriesLastpointIndexRequest);

        sleepSecond(30);

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
                    sleepSecond(1);
                } else {
                    throw ex;
                }
            }
        }
        assertEquals(9, indexRows.size());
        for (int i = 0; i < indexRows.size(); i++) {
            Row row = indexRows.get(i);
            PrimaryKey primaryKey = row.getPrimaryKey();
            assertEquals(primaryKey.getPrimaryKeyColumn("pk1").getValue().asString(), "pk1");
            assertEquals(primaryKey.getPrimaryKeyColumn("pk2").getValue().asLong(), 1000);
            assertEquals(primaryKey.getPrimaryKeyColumn("pk3").getValue().asString(), "pk3_" + (i+1));
            assertEquals(4, row.getColumns().length);
            assertEquals(row.getColumns()[0].getName(), "col1");
            assertEquals(row.getColumns()[0].getValue().asString(), "col1");
            assertEquals(row.getColumns()[1].getName(), "col2:i");
            assertEquals(row.getColumns()[1].getValue().asLong(), 191 + i);
            assertEquals(row.getColumns()[2].getName(), "col3:s");
            assertEquals(row.getColumns()[2].getValue().asString(), "col3");
            assertEquals(row.getColumns()[3].getName(), "pk4");
            assertEquals(row.getColumns()[3].getValue().asLong(), 91+i);
        }
    }

}
