package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.InternalClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableRequestEx;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCreateTableExFunciton {

    static String tableNameNormal = "TestPartitionRange";
    static String tableNameWithPoints = "TestPartitionRange_1";
    static String tableNameWithoutPoints = "TestPartitionRange_2";
    static String tableNameWithTooManyPoints = "TestPartitionRange_3";
    static InternalClient internalClient = null;

    @BeforeClass
    public static void beforClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        internalClient = new InternalClient(endPoint, accessId, accessKey, instanceName);
    }

    @AfterClass
    public static void afterClass() {
        internalClient.shutdown();
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testCreateTableNormal() throws Exception{
        String tableName = tableNameNormal;
        tryDeleteTable(tableName);

        CreateTableResponse createTableResponse = creatTable(tableName);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        DescribeTableResponse describeTableResponse = describeTable(tableName);

        System.out.println(describeTableResponse);
        assertEquals(describeTableResponse.getShardSplits().size(), 0);
    }


    @Test
    public void testCreateTableWithPartitionRange() throws Exception{
        String tableName = tableNameWithPoints;

        tryDeleteTable(tableName);

        List<PrimaryKeyValue> points = Arrays.asList(
                PrimaryKeyValue.fromString("a"),
                PrimaryKeyValue.fromString("b"),
                PrimaryKeyValue.fromString("c"),
                PrimaryKeyValue.fromString("d"),
                PrimaryKeyValue.fromString("e"));

        CreateTableResponse createTableResponse = creatTableEx(tableName ,points);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        DescribeTableResponse describeTableResponse = describeTable(tableName);

        System.out.println(describeTableResponse);
        assertEquals(describeTableResponse.getShardSplits().size(), points.size());
    }

    @Test
    public void testCreateTableWithoutPartitionRange() throws Exception{
        String tableName = tableNameWithoutPoints;

        tryDeleteTable(tableName);

        CreateTableResponse createTableResponse = creatTableEx(tableName, null);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        DescribeTableResponse describeTableResponse = describeTable(tableName);

        System.out.println(describeTableResponse);
        assertEquals(describeTableResponse.getShardSplits().size(), 0);
    }

    @Test
    public void testCreateTableWithTooManyPartitionRange() throws Exception{
        String tableName = tableNameWithTooManyPoints;

        tryDeleteTable(tableName);

        List<PrimaryKeyValue> points = Arrays.asList(
                PrimaryKeyValue.fromString("0"),
                PrimaryKeyValue.fromString("1"),
                PrimaryKeyValue.fromString("2"),
                PrimaryKeyValue.fromString("3"),
                PrimaryKeyValue.fromString("4"),
                PrimaryKeyValue.fromString("5"),
                PrimaryKeyValue.fromString("6"),
                PrimaryKeyValue.fromString("7"),
                PrimaryKeyValue.fromString("8"),
                PrimaryKeyValue.fromString("9"));

        try {
            CreateTableResponse createTableResponse = creatTableEx(tableName, points);
            Assert.fail("Too Many Partition Should Failed");
        } catch (TableStoreException e) {
            Assert.assertEquals(e.getErrorCode(), "OTSParameterInvalid");
            Assert.assertEquals(e.getMessage(), "The count of partitions cannot be greater than 10");
        }
    }


    private void tryDeleteTable(String tableName) throws Exception{
        try {
            deleteTable(tableName);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }
    }

    private DeleteTableResponse deleteTable(String tableName) throws Exception{
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        return internalClient.deleteTable(request, null).get();
    }

    private DescribeTableResponse describeTable(String tableName) throws Exception{
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        return internalClient.describeTable(request, null).get();
    }

    private CreateTableResponse creatTableEx(String tableName, List<PrimaryKeyValue> points) throws Exception{
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK3", PrimaryKeyType.BINARY));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequestEx createTableRequestEx = new CreateTableRequestEx(tableMeta, tableOptions);

        if (points != null) {
            createTableRequestEx.setSplitPoints(points);
        }
        return internalClient.createTableEx(createTableRequestEx, null).get();
    }

    private CreateTableResponse creatTable(String tableName) throws Exception{
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK3", PrimaryKeyType.BINARY));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);


        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        return internalClient.createTable(request, null).get();
    }
}
