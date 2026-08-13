package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.StoragePolicyType;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.TieredStorageColumn;
import com.alicloud.openservices.tablestore.model.TieredStoragePolicy;
import com.alicloud.openservices.tablestore.model.UpdateTableRequest;
import com.alicloud.openservices.tablestore.model.UpdateTableResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class TieredStoragePolicyTest {

    private static final String TABLE_NAME = "TieredStoragePolicyTest";
    private static final String TIME_COLUMN_NAME = "ts_column";
    private static final String TIME_COLUMN_NAME_2 = "ts_column_2";
    private static SyncClient client = null;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        client = new SyncClient(
                settings.getOTSEndpoint(),
                settings.getOTSAccessKeyId(),
                settings.getOTSAccessKeySecret(),
                settings.getOTSInstanceName());
    }

    @AfterClass
    public static void afterClass() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testCreateTableWithTieredStoragePolicy() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_TIMESTAMP);
        policy.setHotRetentionPeriod(7 * 24 * 3600L);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setStoragePolicy(policy);
        client.createTable(request);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();

        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_TIMESTAMP, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNull("Column should be null for BY_TIMESTAMP type", actual.getColumn());

        deleteTable(TABLE_NAME);
    }

    @Test
    public void testUpdateHotRetentionPeriod() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TIME_COLUMN_NAME, PrimaryKeyType.INTEGER));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_COLUMN);
        policy.setHotRetentionPeriod(3 * 24 * 3600L);
        TieredStorageColumn column = new TieredStorageColumn(TIME_COLUMN_NAME);
        policy.setColumn(column);

        CreateTableRequest createRequest = new CreateTableRequest(tableMeta, tableOptions);
        createRequest.setStoragePolicy(policy);
        client.createTable(createRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(Long.valueOf(3 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.MILLISECONDS, actual.getColumn().getValueTimeUnit());

        TieredStoragePolicy updatePolicy = new TieredStoragePolicy(true);
        updatePolicy.setType(StoragePolicyType.SPT_BY_COLUMN);
        updatePolicy.setHotRetentionPeriod(14 * 24 * 3600L);
        TieredStorageColumn updateColumn = new TieredStorageColumn(TIME_COLUMN_NAME);
        updatePolicy.setColumn(updateColumn);
        UpdateTableRequest updateRequest = new UpdateTableRequest(TABLE_NAME);
        updateRequest.setStoragePolicy(updatePolicy);
        updateRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        describeResponse = describeTable(TABLE_NAME);
        actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null after update", actual);
        Assert.assertTrue("enableTieredStorage should still be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(14 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.MILLISECONDS, actual.getColumn().getValueTimeUnit());

        deleteTable(TABLE_NAME);
    }

    @Test
    public void testUpdateStoragePolicyType() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TIME_COLUMN_NAME, PrimaryKeyType.INTEGER));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_TIMESTAMP);
        policy.setHotRetentionPeriod(7 * 24 * 3600L);

        CreateTableRequest createRequest = new CreateTableRequest(tableMeta, tableOptions);
        createRequest.setStoragePolicy(policy);
        client.createTable(createRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_TIMESTAMP, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNull("Column should be null for BY_TIMESTAMP type", actual.getColumn());

        TieredStoragePolicy updatePolicy = new TieredStoragePolicy(true);
        updatePolicy.setType(StoragePolicyType.SPT_BY_COLUMN);
        updatePolicy.setHotRetentionPeriod(7 * 24 * 3600L);
        TieredStorageColumn column = new TieredStorageColumn(TIME_COLUMN_NAME);
        column.setValueTimeUnit(TimeUnit.SECONDS);
        updatePolicy.setColumn(column);

        UpdateTableRequest updateRequest = new UpdateTableRequest(TABLE_NAME);
        updateRequest.setStoragePolicy(updatePolicy);
        updateRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        describeResponse = describeTable(TABLE_NAME);
        actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null after type update", actual);
        Assert.assertTrue("enableTieredStorage should still be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.SECONDS, actual.getColumn().getValueTimeUnit());

//        deleteTable(TABLE_NAME);
    }

    @Test
    public void testUpdateTieredStorageColumnName() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TIME_COLUMN_NAME, PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TIME_COLUMN_NAME_2, PrimaryKeyType.INTEGER));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_COLUMN);
        policy.setHotRetentionPeriod(7 * 24 * 3600L);
        TieredStorageColumn column = new TieredStorageColumn(TIME_COLUMN_NAME);
        column.setValueTimeUnit(TimeUnit.DAYS);
        policy.setColumn(column);

        CreateTableRequest createRequest = new CreateTableRequest(tableMeta, tableOptions);
        createRequest.setStoragePolicy(policy);
        client.createTable(createRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.DAYS, actual.getColumn().getValueTimeUnit());

        TieredStoragePolicy updatePolicy = new TieredStoragePolicy(true);
        updatePolicy.setType(StoragePolicyType.SPT_BY_COLUMN);
        updatePolicy.setHotRetentionPeriod(7 * 24 * 3600L);
        TieredStorageColumn updateColumn = new TieredStorageColumn(TIME_COLUMN_NAME_2);
        updateColumn.setValueTimeUnit(TimeUnit.DAYS);
        updatePolicy.setColumn(updateColumn);

        UpdateTableRequest updateRequest = new UpdateTableRequest(TABLE_NAME);
        updateRequest.setStoragePolicy(updatePolicy);
        updateRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        describeResponse = describeTable(TABLE_NAME);
        actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null after column name update", actual);
        Assert.assertTrue("enableTieredStorage should still be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME_2, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.DAYS, actual.getColumn().getValueTimeUnit());

        deleteTable(TABLE_NAME);
    }

    @Test
    public void testUpdateTieredStorageColumnTimeUnit() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(TIME_COLUMN_NAME, PrimaryKeyType.INTEGER));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_COLUMN);
        policy.setHotRetentionPeriod(7 * 24 * 3600L);
        TieredStorageColumn column = new TieredStorageColumn(TIME_COLUMN_NAME);
        column.setValueTimeUnit(TimeUnit.MICROSECONDS);
        policy.setColumn(column);

        CreateTableRequest createRequest = new CreateTableRequest(tableMeta, tableOptions);
        createRequest.setStoragePolicy(policy);
        client.createTable(createRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.MICROSECONDS, actual.getColumn().getValueTimeUnit());

        TieredStoragePolicy updatePolicy = new TieredStoragePolicy(true);
        updatePolicy.setType(StoragePolicyType.SPT_BY_COLUMN);
        updatePolicy.setHotRetentionPeriod(7 * 24 * 3600L);
        TieredStorageColumn updateColumn = new TieredStorageColumn(TIME_COLUMN_NAME);
        updateColumn.setValueTimeUnit(TimeUnit.NANOSECONDS);
        updatePolicy.setColumn(updateColumn);

        UpdateTableRequest updateRequest = new UpdateTableRequest(TABLE_NAME);
        updateRequest.setStoragePolicy(updatePolicy);
        updateRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        describeResponse = describeTable(TABLE_NAME);
        actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null after TimeUnit update", actual);
        Assert.assertTrue("enableTieredStorage should still be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_COLUMN, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNotNull("Column should not be null", actual.getColumn());
        Assert.assertEquals(TIME_COLUMN_NAME, actual.getColumn().getName());
        Assert.assertEquals(TimeUnit.NANOSECONDS, actual.getColumn().getValueTimeUnit());

        deleteTable(TABLE_NAME);
    }

    @Test
    public void testDisableTieredStoragePolicy() throws Exception {
        tryDeleteTable(TABLE_NAME);

        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));

        TableOptions tableOptions = new TableOptions(-1, 1);

        TieredStoragePolicy policy = new TieredStoragePolicy(true);
        policy.setType(StoragePolicyType.SPT_BY_TIMESTAMP);
        policy.setHotRetentionPeriod(7 * 24 * 3600L);

        CreateTableRequest createRequest = new CreateTableRequest(tableMeta, tableOptions);
        createRequest.setStoragePolicy(policy);
        client.createTable(createRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(2));

        DescribeTableResponse describeResponse = describeTable(TABLE_NAME);
        TieredStoragePolicy actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null", actual);
        Assert.assertTrue("enableTieredStorage should be true", actual.isEnableTieredStorage());
        Assert.assertEquals(StoragePolicyType.SPT_BY_TIMESTAMP, actual.getType());
        Assert.assertEquals(Long.valueOf(7 * 24 * 3600L), actual.getHotRetentionPeriod());
        Assert.assertNull("Column should be null for BY_TIMESTAMP type", actual.getColumn());

        TieredStoragePolicy disablePolicy = new TieredStoragePolicy(false);
        UpdateTableRequest updateRequest = new UpdateTableRequest(TABLE_NAME);
        updateRequest.setStoragePolicy(disablePolicy);
        updateRequest.setTableOptionsForUpdate(tableOptions);
        client.updateTable(updateRequest);

        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        describeResponse = describeTable(TABLE_NAME);
        actual = describeResponse.getStoragePolicy();
        Assert.assertNotNull("StoragePolicy should not be null after disable", actual);
        Assert.assertFalse("enableTieredStorage should be false", actual.isEnableTieredStorage());
        Assert.assertNull("Type should be null after disable", actual.getType());
        Assert.assertNull("HotRetentionPeriod should be null after disable", actual.getHotRetentionPeriod());
        Assert.assertNull("Column should be null after disable", actual.getColumn());

        deleteTable(TABLE_NAME);
    }

    private void tryDeleteTable(String tableName) throws Exception {
        try {
            deleteTable(tableName);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }
    }

    private DeleteTableResponse deleteTable(String tableName) throws Exception {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        return client.deleteTable(request);
    }

    private DescribeTableResponse describeTable(String tableName) throws Exception {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        return client.describeTable(request);
    }
}
