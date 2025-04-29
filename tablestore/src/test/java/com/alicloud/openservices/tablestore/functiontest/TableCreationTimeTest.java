package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class TableCreationTimeTest {
    static String testTableCreationTime = "TestTableCreationTime";
    static SyncClient client = null;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new SyncClient(endPoint, accessId, accessKey, instanceName);
    }

    @AfterClass
    public static void afterClass() {
        client.shutdown();
    }

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testCreateTableNormal() throws Exception{
        String tableName = testTableCreationTime;
        tryDeleteTable(tableName);
        long start = System.currentTimeMillis();

        CreateTableResponse createTableResponse = creatTable(tableName);
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        DescribeTableResponse describeTableResponse = describeTable(tableName);

        System.out.println(describeTableResponse);
        // There is a possibility that the test machine is not time-synchronized with the server, so reserve a 60-second buffer.
        Assert.assertTrue(describeTableResponse.getCreationTime() > (start - 60 * 1000) * 1000);
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
        return client.deleteTable(request);
    }

    private DescribeTableResponse describeTable(String tableName) throws Exception{
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        return client.describeTable(request);
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

        return client.createTable(request);
    }
}
