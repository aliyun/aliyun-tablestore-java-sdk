package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.alicloud.openservices.tablestore.model.sql.SQLResultSet;
import com.alicloud.openservices.tablestore.model.sql.SQLRow;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimeseriesAnalyticalStoreTest {

    static TimeseriesClient client = null;
    static SyncClient tableStoreClient = null;

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
        deleteTable("test_analytical_store1");
        deleteTable("test_analytical_store2");
        deleteTable("test_analytical_store3");
        deleteTable("test_create_and_delete_analytical_store1");
        deleteTable("test_create_and_delete_analytical_store2");
        deleteTable("test_update_analytical_store");
        client.shutdown();
        tableStoreClient.shutdown();
    }

    static void deleteTable(String tableName) {
        try {
            client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(tableName));
        } catch (Exception ignore) {}
    }

    @Test
    public void testCreateTable() {
        // create table with default analytical store
        TimeseriesTableMeta meta = new TimeseriesTableMeta("test_analytical_store1");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        // describe table
        DescribeTimeseriesTableRequest describeTableRequest = new DescribeTimeseriesTableRequest("test_analytical_store1");
        DescribeTimeseriesTableResponse describeTableResponse = client.describeTimeseriesTable(describeTableRequest);
        List<TimeseriesAnalyticalStore> analyticalStores = describeTableResponse.getAnalyticalStores();
        Assert.assertEquals(1, analyticalStores.size());
        Assert.assertEquals("default_analytical_store", analyticalStores.get(0).getAnalyticalStoreName());
        Assert.assertEquals(-1, analyticalStores.get(0).getTimeToLive());
        Assert.assertEquals(AnalyticalStoreSyncType.SYNC_TYPE_FULL, analyticalStores.get(0).getSyncOption());
        // describe analytical store
        DescribeTimeseriesAnalyticalStoreRequest describeRequest = new DescribeTimeseriesAnalyticalStoreRequest("test_analytical_store1", "default_analytical_store");
        DescribeTimeseriesAnalyticalStoreResponse describeResponse = client.describeTimeseriesAnalyticalStore(describeRequest);
        Assert.assertEquals( "default_analytical_store", describeResponse.getAnalyticalStore().getAnalyticalStoreName());
        Assert.assertEquals( -1, describeResponse.getAnalyticalStore().getTimeToLive());
        Assert.assertEquals( AnalyticalStoreSyncType.SYNC_TYPE_FULL, describeResponse.getAnalyticalStore().getSyncOption());
        Assert.assertEquals(AnalyticalStoreSyncType.SYNC_TYPE_INCR, describeResponse.getSyncStat().getSyncPhase());
        Assert.assertEquals(0, describeResponse.getSyncStat().getCurrentSyncTimestamp());
        Assert.assertNull(describeResponse.getStorageSize());

        // create table without analytical store
        meta = new TimeseriesTableMeta("test_analytical_store2");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        createTimeseriesTableRequest.setEnableAnalyticalStore(false);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        // describe table
        describeTableRequest = new DescribeTimeseriesTableRequest("test_analytical_store2");
        describeTableResponse = client.describeTimeseriesTable(describeTableRequest);
        analyticalStores = describeTableResponse.getAnalyticalStores();
        Assert.assertEquals(0, analyticalStores.size());

        // create table with custom analytical store
        meta = new TimeseriesTableMeta("test_analytical_store3");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        TimeseriesAnalyticalStore analyticalStore = new TimeseriesAnalyticalStore("custom_analytical_store");
        analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_FULL);
        analyticalStore.setTimeToLive(86400);
        List<TimeseriesAnalyticalStore> analyticalStoreList = new ArrayList<TimeseriesAnalyticalStore>();
        analyticalStoreList.add(analyticalStore);
        createTimeseriesTableRequest.setAnalyticalStores(analyticalStoreList);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        // describe table
        describeTableRequest = new DescribeTimeseriesTableRequest("test_analytical_store3");
        describeTableResponse = client.describeTimeseriesTable(describeTableRequest);
        analyticalStores = describeTableResponse.getAnalyticalStores();
        Assert.assertEquals(1, analyticalStores.size());
        Assert.assertEquals("custom_analytical_store", analyticalStores.get(0).getAnalyticalStoreName());
        Assert.assertEquals(86400, analyticalStores.get(0).getTimeToLive());
        Assert.assertEquals(AnalyticalStoreSyncType.SYNC_TYPE_FULL, analyticalStores.get(0).getSyncOption());
    }

    @Test
    public void testCreateAndDelete() {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta("test_create_and_delete_analytical_store1");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        createTimeseriesTableRequest.setEnableAnalyticalStore(false);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        meta = new TimeseriesTableMeta("test_create_and_delete_analytical_store2");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        createTimeseriesTableRequest.setEnableAnalyticalStore(false);
        client.createTimeseriesTable(createTimeseriesTableRequest);

        // create full sync analytical store
        TimeseriesAnalyticalStore analyticalStore = new TimeseriesAnalyticalStore("full_sync_analytical_store");
        analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_FULL);
        analyticalStore.setTimeToLive(-1);
        CreateTimeseriesAnalyticalStoreRequest createRequest = new CreateTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", analyticalStore);
        client.createTimeseriesAnalyticalStore(createRequest);
        DescribeTimeseriesAnalyticalStoreRequest describeRequest = new DescribeTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", "full_sync_analytical_store");
        DescribeTimeseriesAnalyticalStoreResponse describeResponse = client.describeTimeseriesAnalyticalStore(describeRequest);
        Assert.assertEquals( "full_sync_analytical_store", describeResponse.getAnalyticalStore().getAnalyticalStoreName());
        Assert.assertEquals( -1, describeResponse.getAnalyticalStore().getTimeToLive());
        Assert.assertEquals( AnalyticalStoreSyncType.SYNC_TYPE_FULL, describeResponse.getAnalyticalStore().getSyncOption());
        Assert.assertEquals(AnalyticalStoreSyncType.SYNC_TYPE_FULL, describeResponse.getSyncStat().getSyncPhase());
        Assert.assertEquals(0, describeResponse.getSyncStat().getCurrentSyncTimestamp());
        Assert.assertNull(describeResponse.getStorageSize());

        // delete analytical store without mapping table
        DeleteTimeseriesAnalyticalStoreRequest deleteRequest = new DeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store1", "full_sync_analytical_store");
        client.deleteTimeseriesAnalyticalStore(deleteRequest);

        // create incremental sync analytical store
        analyticalStore = new TimeseriesAnalyticalStore("incr_sync_analytical_store");
        analyticalStore.setSyncOption(AnalyticalStoreSyncType.SYNC_TYPE_INCR);
        analyticalStore.setTimeToLive(86400);
        createRequest = new CreateTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", analyticalStore);
        client.createTimeseriesAnalyticalStore(createRequest);
        describeRequest = new DescribeTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store");
        describeResponse = client.describeTimeseriesAnalyticalStore(describeRequest);
        Assert.assertEquals( "incr_sync_analytical_store", describeResponse.getAnalyticalStore().getAnalyticalStoreName());
        Assert.assertEquals( 86400, describeResponse.getAnalyticalStore().getTimeToLive());
        Assert.assertEquals( AnalyticalStoreSyncType.SYNC_TYPE_INCR, describeResponse.getAnalyticalStore().getSyncOption());
        Assert.assertEquals(AnalyticalStoreSyncType.SYNC_TYPE_INCR, describeResponse.getSyncStat().getSyncPhase());
        Assert.assertEquals(0, describeResponse.getSyncStat().getCurrentSyncTimestamp());
        Assert.assertNull(describeResponse.getStorageSize());

        // create sql mapping table
        SQLQueryRequest sqlQueryRequest = new SQLQueryRequest("CREATE TABLE `test_create_and_delete_analytical_store2::cpu` (" +
            "`_m_name` varchar(1024) NOT NULL," +
            "`_data_source` varchar(1024) NOT NULL," +
            "`_tags` varchar(1024) NOT NULL," +
            "`_time` bigint(20) NOT NULL," +
            "PRIMARY KEY (`_m_name`,`_data_source`,`_tags`,`_time`)" +
            ") ENGINE=AnalyticalStore");
        tableStoreClient.sqlQuery(sqlQueryRequest);

        // delete analytical store with mapping table
        try {
            deleteRequest = new DeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store");
            client.deleteTimeseriesAnalyticalStore(deleteRequest);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.toString().contains("OTSParameterInvalid"));
        }

        // delete analytical store and drop mapping table
        deleteRequest = new DeleteTimeseriesAnalyticalStoreRequest("test_create_and_delete_analytical_store2", "incr_sync_analytical_store");
        deleteRequest.setDropMappingTable(true);
        client.deleteTimeseriesAnalyticalStore(deleteRequest);
        SQLQueryResponse showTablesResponse = tableStoreClient.sqlQuery(new SQLQueryRequest("SHOW TABLES"));
        SQLResultSet resultSet = showTablesResponse.getSQLResultSet();
        while (resultSet.hasNext()) {
            SQLRow row = resultSet.next();
            Assert.assertFalse(row.getString(0).equals("test_create_and_delete_analytical_store2::cpu"));
        }
    }

    @Test
    public void testUpdate() {
        // create table
        TimeseriesTableMeta meta = new TimeseriesTableMeta("test_update_analytical_store");
        meta.setTimeseriesMetaOptions(new TimeseriesMetaOptions());
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(meta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
        DescribeTimeseriesAnalyticalStoreRequest describeRequest = new DescribeTimeseriesAnalyticalStoreRequest("test_update_analytical_store", "default_analytical_store");
        DescribeTimeseriesAnalyticalStoreResponse describeResponse = client.describeTimeseriesAnalyticalStore(describeRequest);
        Assert.assertEquals( "default_analytical_store", describeResponse.getAnalyticalStore().getAnalyticalStoreName());
        Assert.assertEquals( -1, describeResponse.getAnalyticalStore().getTimeToLive());

        // update analytical store
        TimeseriesAnalyticalStore analyticalStore = new TimeseriesAnalyticalStore("default_analytical_store");
        analyticalStore.setTimeToLive(86400);
        UpdateTimeseriesAnalyticalStoreRequest updateRequest = new UpdateTimeseriesAnalyticalStoreRequest("test_update_analytical_store");
        updateRequest.setAnalyticStore(analyticalStore);
        client.updateTimeseriesAnalyticalStore(updateRequest);
        describeResponse = client.describeTimeseriesAnalyticalStore(describeRequest);
        Assert.assertEquals( "default_analytical_store", describeResponse.getAnalyticalStore().getAnalyticalStoreName());
        Assert.assertEquals( 86400, describeResponse.getAnalyticalStore().getTimeToLive());
    }
}
