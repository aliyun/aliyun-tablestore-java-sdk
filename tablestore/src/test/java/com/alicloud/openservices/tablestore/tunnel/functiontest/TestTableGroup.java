package com.alicloud.openservices.tablestore.tunnel.functiontest;


import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.tunnel.*;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author lihn
 * @Date 2022/7/15 21:40
 */
public class TestTableGroup {

    private static Logger logger = LoggerFactory.getLogger(TestTableGroup.class);


    private static TunnelClient tunnelClient = null;
    private static SyncClient syncClient = null;
    private static String tableName = "testtable" + System.currentTimeMillis();
    private static String tunnelName = "test" + System.currentTimeMillis();



    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();
        tunnelClient = new TunnelClient(endPoint, accessId, accessKey, instanceName);
        syncClient = new SyncClient(endPoint, accessId, accessKey, instanceName);
    }


    @After
    public void tearDown() {
        DeleteTunnelRequest r = new DeleteTunnelRequest(tableName, tunnelName);
        tunnelClient.deleteTunnel(r);
    }

    @AfterClass
    public static void afterClass() {
        if (syncClient != null) {
            syncClient.shutdown();
        }
        if (tunnelClient != null) {
            tunnelClient.shutdown();
        }
    }

    @Test
    public void testTableGroup() throws Exception {
        try {
            //  1. create table
            System.out.println("Begin Create Table: " + tableName);
            createTable(syncClient, tableName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  2. create tunnel
            System.out.println("Begin Create Tunnel: " + tunnelName);
            TestTunnelLiveTail.createTunnel(tunnelClient, tableName, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  3. put data
            logger.info("Begin Put Data");
            putRows(syncClient, tableName, "pk0", 10000);
            logger.info("++++++++++++++++++++++++++++++++++++");
            logger.info("Waiting for channel ready...........");
            Thread.sleep(400000);

            //  4. describe tunnel
            logger.info("Begin Describe Tunnel: " + tunnelName);
            DescribeTunnelResponse describeTunnelResponse = TestTunnelLiveTail.describeTunnel(tunnelClient, tableName, tunnelName);
            logger.info("++++++++++++++++++++++++++++++++++++");

            //  5. get checkpoint
            logger.info("Begin GetCheckpoint");
            TunnelInfo tunnelInfo = describeTunnelResponse.getTunnelInfo();
            if (describeTunnelResponse.getChannelInfos() != null && !describeTunnelResponse.getChannelInfos().isEmpty()) {
                logger.info("size:"+ describeTunnelResponse.getChannelInfos().size());
            } else {
                logger.info("size:" + 0);
                Assert.fail("channel list is empty.");
            }
            for (ChannelInfo channelInfo : describeTunnelResponse.getChannelInfos()) {
                GetCheckpointResponse getCheckpointResponse =
                        TestTunnelLiveTail.getcheckpoint(tunnelClient, tunnelInfo.getTunnelId(), channelInfo.getClientId(),
                                channelInfo.getChannelId());
                //  6. read some records
                logger.info("Begin Read Records");
                ReadRecordsRequest request = new ReadRecordsRequest(tunnelInfo.getTunnelId(), channelInfo.getClientId(),
                        channelInfo.getChannelId(), getCheckpointResponse.getCheckpoint());
                Boolean preMayMoreRecord = null;

                for (int count = 1; count < 20; count++) {
                    logger.info("Read Records, Round: " + count);
                    ReadRecordsResponse resp = tunnelClient.readRecords(request);
                    if (preMayMoreRecord != null && !preMayMoreRecord) {
                        Assert.assertTrue(resp.getRecords() == null || resp.getRecords().isEmpty());
                    }

                    preMayMoreRecord = resp.getMayMoreRecord();
                    logger.info(resp.getMayMoreRecord() + "");
                    logger.info(resp.getRecords().size() + "");
                    logger.info(resp.getNextToken());
                    logger.info("++++++++++++++++++++++++++++++++++++");
                    if (resp.getNextToken() == null) {
                        break;
                    }
                    request.setToken(resp.getNextToken());
                    Thread.sleep(2000);
                }

            }
            logger.info("finish test.");

        } catch (Exception e) {
            logger.error("test fail.", e);
            Assert.fail();
        }

    }


    static void putRows(SyncClient client, String tableName, String pk, int number) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int i = 0; i < number; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString(i+""));
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKeyBuilder.build());

            for (int j = 0; j < 1; j++) {
                rowPutChange.addColumn(new Column("test" + j, ColumnValue.fromLong(i)));
            }

            batchWriteRowRequest.addRowChange(rowPutChange);

            if (batchWriteRowRequest.getRowsCount() >= 10 || i == number) {
                client.batchWriteRow(batchWriteRowRequest);
                batchWriteRowRequest = new BatchWriteRowRequest();
            }
            if (i % 1000 == 0) {
                logger.info("i:" + i);
            }
        }
        logger.info("Put 10000 rows succeed.");

    }


    private static void createTable(SyncClient client, String tableName) {
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumn("pk0", PrimaryKeyType.STRING);
        CreateTableRequest request = new CreateTableRequest(meta, new TableOptions(-1, 1));
        client.createTable(request);
    }


}
