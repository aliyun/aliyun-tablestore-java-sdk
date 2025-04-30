package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.*;

import java.util.concurrent.TimeUnit;

public class TestComputeSplitPointsBySize {
    static String tableName = "YSTestComputeSplit";
    static SyncClient publicClient = null;

    static PrimaryKey lowerBound = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.INF_MIN)
            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN).build();

    static PrimaryKey upperBound = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("PK1", PrimaryKeyValue.INF_MAX)
            .addPrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MAX).build();

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String publicInstanceName = settings.getOTSInstanceName();

        publicClient = new SyncClient(endPoint, accessId, accessKey, publicInstanceName);
    }

    @AfterClass
    public static void afterClass() {
        publicClient.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        try {
            deleteTable(publicClient);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }

        creatTable(publicClient);

        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    private void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        client.deleteTable(request);
    }

    private void creatTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    /**
     * Note: To ensure the case runs correctly:
     * 1. Set sqlonline_ots_MaxSplitPointNum to 10 in OTSServer.
     * 2. Set sqlonline_ots_MinSplitSizeUnitInBytes to 100 in OTSServer.
     */
    @Test
    public void testBase() {
        {
            for (int i = 0; i < 1000; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(String.format("%05d", i)))
                        .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i)).build();
                RowPutChange rowChange = new RowPutChange(tableName);
                rowChange.setPrimaryKey(pk);
                rowChange.addColumn("attr0", ColumnValue.fromString("Hello"));
                rowChange.addColumn("attr1", ColumnValue.fromString("Hello"));
                rowChange.addColumn("attr2", ColumnValue.fromString("Hello"));
                rowChange.addColumn("attr3", ColumnValue.fromString("Hello"));
                rowChange.addColumn("attr4", ColumnValue.fromString("Hello"));
                PutRowRequest request = new PutRowRequest(rowChange);
                publicClient.putRow(request);
            }
        }

        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeInByte(1, 100 * 1024 * 1024); // 1KB
            ComputeSplitsBySizeResponse response = publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
            Assert.assertEquals(1, response.getSplits().size());
            Assert.assertEquals(1, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            Assert.assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
        }
    }

    @Test
    public void testEmptyTable() {
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeIn100MB(1);
            ComputeSplitsBySizeResponse response = publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
            Assert.assertEquals(1, response.getSplits().size());
            Assert.assertEquals(1, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            Assert.assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());

            Split split = response.getSplits().get(0);
            Assert.assertEquals(32,  split.getLocation().length());
            Assert.assertEquals(lowerBound,  split.getLowerBound());
            Assert.assertEquals(upperBound,  split.getUpperBound());
        }
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeIn100MB(1000);
            computeSplitsBySizeRequest.setSplitPointLimit(1);
            ComputeSplitsBySizeResponse response = publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
            Assert.assertEquals(1, response.getSplits().size());
            Assert.assertEquals(1, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            Assert.assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());

            Split split = response.getSplits().get(0);
            Assert.assertEquals(32,  split.getLocation().length());
            Assert.assertEquals(lowerBound,  split.getLowerBound());
            Assert.assertEquals(upperBound,  split.getUpperBound());
        }
    }

    /**
     * Note: To ensure the case runs correctly:
     * 1. Set sqlonline_ots_MaxSplitPointNum to 10 in OTSServer.
     * 2. Set sqlonline_ots_MinSplitSizeUnitInBytes to 100 in OTSServer.
     */
    @Test @Ignore
    public void testTouchLimit() {
        {
            String v = String.format("%01000d", 100);
            for (int i = 0; i < 50000; i++) {
                PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString(String.format("%05d", i)))
                        .addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i)).build();
                RowPutChange rowChange = new RowPutChange(tableName);
                rowChange.setPrimaryKey(pk);
                rowChange.addColumn("attr0", ColumnValue.fromString(v));
                rowChange.addColumn("attr1", ColumnValue.fromString(v));
                rowChange.addColumn("attr2", ColumnValue.fromString(v));
                rowChange.addColumn("attr3", ColumnValue.fromString(v));
                rowChange.addColumn("attr4", ColumnValue.fromString(v));
                PutRowRequest request = new PutRowRequest(rowChange);
                publicClient.putRow(request);
            }
        }

        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeInByte(1, 1024 * 1024); // 1MB
            computeSplitsBySizeRequest.setSplitPointLimit(2);
            ComputeSplitsBySizeResponse response = publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
            Assert.assertEquals(3, response.getSplits().size());
            Assert.assertEquals(4, response.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
            Assert.assertEquals(0, response.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());

            PrimaryKey p1 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("27908\u0000"))
                    .addPrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN).build();

            PrimaryKey p2 = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("41420\u0000"))
                    .addPrimaryKeyColumn("PK2", PrimaryKeyValue.INF_MIN).build();

            Assert.assertEquals(32, response.getSplits().get(0).getLocation().length());
            Assert.assertEquals(lowerBound, response.getSplits().get(0).getLowerBound());
            Assert.assertEquals(p1, response.getSplits().get(0).getUpperBound());
            Assert.assertEquals(32, response.getSplits().get(1).getLocation().length());
            Assert.assertEquals(p1, response.getSplits().get(1).getLowerBound());
            Assert.assertEquals(p2, response.getSplits().get(1).getUpperBound());
            Assert.assertEquals(32, response.getSplits().get(2).getLocation().length());
            Assert.assertEquals(p2, response.getSplits().get(2).getLowerBound());
            Assert.assertEquals(upperBound, response.getSplits().get(2).getUpperBound());
        }
    }

    @Test
    public void testInvalidParam() {
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName("");
            computeSplitsBySizeRequest.setSplitSizeIn100MB(1);
            try {
                publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                Assert.assertEquals("The table name for ComputeSplitsBySize should not be null or empty.", e.getMessage());
            }

        }
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeIn100MB(0);
            try {
                publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                Assert.assertEquals("The split_size should be greater than 0.", e.getMessage());
            }
        }
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeInByte(0, 100 * 1024 * 1024);
            try {
                publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                Assert.assertEquals("The split_size should be greater than 0.", e.getMessage());
            }
        }
        {
            ComputeSplitsBySizeRequest computeSplitsBySizeRequest = new ComputeSplitsBySizeRequest();
            computeSplitsBySizeRequest.setTableName(tableName);
            computeSplitsBySizeRequest.setSplitSizeInByte(1000, 0);
            try {
                publicClient.computeSplitsBySize(computeSplitsBySizeRequest);
                Assert.fail();
            } catch (TableStoreException e) {
                Assert.assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
                // fir
                Assert.assertEquals("The split_size_unit_in_byte should be greater equal than: 1048576", e.getMessage());
            }
        }
    }
}
