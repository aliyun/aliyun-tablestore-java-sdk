package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.tunnel.*;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
//import com.sun.corba.se.impl.orbutil.concurrent.Sync;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTunnelConsumer {
    private static SyncClient syncClient;
    private static TunnelClient tunnelClient;
    private static TunnelWorkerConfig workerConfig;

    private final String TABLE_NAME = "test_tunnel_consumer";
    private final String TUNNEL_NAME = "test_tunnel_consumer";
    // For Assert
    private static ConcurrentHashMap<PrimaryKey, StreamRecord> totalRecords =
            new ConcurrentHashMap<PrimaryKey, StreamRecord>();
    private static ArrayList<StreamRecord> totalStreamRecordsWithOrigin =
            new ArrayList<StreamRecord>();
    private static AtomicLong consumeRound = new AtomicLong(0);

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        syncClient = new SyncClient(endPoint, accessId, accessKey, instanceName);
        tunnelClient = new TunnelClient(endPoint, accessId, accessKey, instanceName);
        workerConfig = new TunnelWorkerConfig(new SimpleProcessor());
    }

    @AfterClass
    public static void afterClass() {
        if (syncClient != null) {
            syncClient.shutdown();
        }
        if (tunnelClient != null) {
            tunnelClient.shutdown();
        }
        if (workerConfig != null) {
            workerConfig.shutdown();
        }
    }

    @Before
    public void setUp() throws Exception {
        totalRecords.clear();
        totalStreamRecordsWithOrigin.clear();
        workerConfig.setMaxChannelParallel(-1);
        workerConfig.setReadMaxTimesPerRound(1);
        workerConfig.setReadMaxBytesPerRound(4 * 1024 * 1024);

        try {
            deleteTunnel(tunnelClient);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.INVALID_PARAMETER)) {
                throw e;
            }
        }
        try {
            deleteTable(syncClient);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }

        createTable(syncClient);
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    @After
    public void tearDown() {

    }

    private void deleteTunnel(TunnelClient client) {
        DeleteTunnelRequest deleteTunnelRequest = new DeleteTunnelRequest(TABLE_NAME, TUNNEL_NAME);
        client.deleteTunnel(deleteTunnelRequest);
    }

    private void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private String createTunnel(TunnelClient client, TunnelType type) {
        CreateTunnelRequest request = new CreateTunnelRequest(TABLE_NAME, TUNNEL_NAME, type);
        CreateTunnelResponse resp = client.createTunnel(request);
        return resp.getTunnelId();
    }

    private void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK1", PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("PK2", PrimaryKeyType.INTEGER));
        int timeToLive = -1;
        int maxVersions = 1;
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        client.createTable(request);
    }

    private void updateTableWithSetOriginColumn(SyncClient client, String column) {
        UpdateTableRequest request = new UpdateTableRequest(TABLE_NAME);
        StreamSpecification streamSpecification = new StreamSpecification(true, 168);
        streamSpecification.addOriginColumnsToGet(column);
        request.setStreamSpecification(streamSpecification);
        client.updateTable(request);
    }

    private void prepareTable(SyncClient client, int rowCount) {
        // prepare via BatchWriteRow
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int i = 0; i < rowCount; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("Str" + i));
            primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
            RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKeyBuilder.build());
            batchWriteRowRequest.addRowChange(rowPutChange);
            if (batchWriteRowRequest.getRowsCount() == 200) {
                client.batchWriteRow(batchWriteRowRequest);
                batchWriteRowRequest = new BatchWriteRowRequest();
            }
        }
        if (batchWriteRowRequest.getRowsCount() > 0) {
            client.batchWriteRow(batchWriteRowRequest);
        }
    }

    private void putAndUpdateRow(SyncClient client, String column) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("PK1"));
        primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(1));
        RowPutChange putRowChange = new RowPutChange(TABLE_NAME, primaryKeyBuilder.build());
        PutRowRequest putRowRequest = new PutRowRequest(putRowChange);
        putRowChange.addColumn(new Column(column, ColumnValue.fromString(column + "ori")));
        client.putRow(putRowRequest);

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKeyBuilder.build());
        rowUpdateChange.put(new Column(column, ColumnValue.fromString(column + "tar")));
        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    static class SimpleProcessor implements IChannelProcessor {
        @Override
        public void process(ProcessRecordsInput input) {
            for (StreamRecord streamRecord : input.getRecords()) {
                totalRecords.put(streamRecord.getPrimaryKey(), streamRecord);
                totalStreamRecordsWithOrigin.add(streamRecord);
            }
            consumeRound.addAndGet(1);
        }

        @Override
        public void shutdown() {
            System.out.println("Mock shutdown");
        }
    }

    @Test
    public void testFuzzyConsume() throws Exception {
        // 1. 随机写入一定数量数据
        int rowCount = new Random(System.currentTimeMillis()).nextInt(100000) + 1;
        prepareTable(syncClient, rowCount);
        System.out.println("Finish prepare table.");
        // 2. 创建全量类型的Tunnel开始消费，Tunnel进入增量状态时表示消费完毕
        String tunnelId = createTunnel(tunnelClient, TunnelType.BaseData);
        TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            tunnelWorker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 3. 等待数据消费完毕
        DescribeTunnelResponse describeTunnelResponse = tunnelClient.describeTunnel(
                new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        while (describeTunnelResponse.getTunnelInfo().getStage() != TunnelStage.ProcessStream) {
            Thread.sleep(1000);
            describeTunnelResponse = tunnelClient.describeTunnel(
                    new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        }
        System.out.println("finish consume all data");

        // 4. Assert with totalRecords
        assertEquals(rowCount, totalRecords.size());
        for (int i = 0; i < rowCount; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("Str" + i));
            primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
            PrimaryKey pk = primaryKeyBuilder.build();
            assertTrue(totalRecords.containsKey(pk));
        }
    }

    @Test
    public void testMultiTimesReadConsume() throws Exception {
        // 1. 随机写入一定数量数据
        Random rand = new Random(System.currentTimeMillis());
        int rowCount = rand.nextInt(100000) + 5000;
        prepareTable(syncClient, rowCount);
        System.out.println("Finish prepare table.");

        // 2. 创建全量类型的Tunnel开始消费，Tunnel进入增量状态时表示消费完毕
        workerConfig.setReadMaxTimesPerRound(rand.nextInt(10) + 1);
        workerConfig.setReadMaxBytesPerRound((rand.nextInt(50) + 4) * 1024 * 1024);
        workerConfig.setHeartbeatIntervalInSec(10);
        String tunnelId = createTunnel(tunnelClient, TunnelType.BaseData);
        TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            tunnelWorker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 3. 等待数据消费完毕
        DescribeTunnelResponse describeTunnelResponse = tunnelClient.describeTunnel(
                new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        while (describeTunnelResponse.getTunnelInfo().getStage() != TunnelStage.ProcessStream) {
            Thread.sleep(1000);
            describeTunnelResponse = tunnelClient.describeTunnel(
                    new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        }
        System.out.println("finish consume all data");

        // 4. Assert with totalRecords
        assertEquals(rowCount, totalRecords.size());
        for (int i = 0; i < rowCount; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("Str" + i));
            primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
            PrimaryKey pk = primaryKeyBuilder.build();
            assertTrue(totalRecords.containsKey(pk));
        }
    }

    @Test
    public void testMaxParallelConsume() throws Exception {
        // 1. 随机写入一定数量数据
        int rowCount = new Random(System.currentTimeMillis()).nextInt(100000) + 1;
        prepareTable(syncClient, rowCount);
        System.out.println("Finish prepare table.");
        // 2. 创建全量类型的Tunnel开始消费，Tunnel进入增量状态时表示消费完毕
        workerConfig.setMaxChannelParallel(1);
        workerConfig.setHeartbeatIntervalInSec(10);
        String tunnelId = createTunnel(tunnelClient, TunnelType.BaseData);
        TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            tunnelWorker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 3. 等待数据消费完毕
        DescribeTunnelResponse describeTunnelResponse = tunnelClient.describeTunnel(
                new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        while (describeTunnelResponse.getTunnelInfo().getStage() != TunnelStage.ProcessStream) {
            Thread.sleep(1000);
            describeTunnelResponse = tunnelClient.describeTunnel(
                    new DescribeTunnelRequest(TABLE_NAME, TUNNEL_NAME));
        }
        System.out.println("finish consume all data");

        // 4. Assert with totalRecords
        assertEquals(rowCount, totalRecords.size());
        for (int i = 0; i < rowCount; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("Str" + i));
            primaryKeyBuilder.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(i));
            PrimaryKey pk = primaryKeyBuilder.build();
            assertTrue(totalRecords.containsKey(pk));
        }
    }

    @Test
    public void testConsumeOriginColumn() throws Exception {
        String column = "col1";
        //1. 为表设置原始列
        updateTableWithSetOriginColumn(syncClient, column);
        Random rand = new Random(System.currentTimeMillis());

        // 2. 创建增量类型的Tunnel开始消费，Tunnel读出来数据后assert
        workerConfig.setReadMaxTimesPerRound(rand.nextInt(10) + 1);
        workerConfig.setReadMaxBytesPerRound((rand.nextInt(50) + 4) * 1024 * 1024);
        workerConfig.setHeartbeatIntervalInSec(10);
        String tunnelId = createTunnel(tunnelClient, TunnelType.Stream);
        TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            tunnelWorker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 3. 写入一条数据后再做更新
        putAndUpdateRow(syncClient, column);

        // 4. 等待数据消费完毕
        long beginTs = System.currentTimeMillis();
        long count = consumeRound.get();
        long cost = 0;

        for(;;) {
            if (consumeRound.get() - count > 0) {
                break;
            }
            if (cost > 2 * 60 * 1000) {
                break;
            }
            Thread.sleep(5000);
            cost = System.currentTimeMillis() - beginTs;
        }

        // 5. Assert with totalStreamRecordsWithOrigin
        assertEquals(2, totalStreamRecordsWithOrigin.size());
        long  pk2Val = 1;
        for (int i = 0; i < totalStreamRecordsWithOrigin.size(); i++) {
            StreamRecord streamRecord = totalStreamRecordsWithOrigin.get(i);
            //assert pk equal
            PrimaryKeyColumn[] primaryKeyColumns = streamRecord.getPrimaryKey().getPrimaryKeyColumns();
            assertEquals(primaryKeyColumns.length, 2);
            assertEquals("PK1", primaryKeyColumns[0].getName());
            assertEquals("PK1", primaryKeyColumns[0].getValue().asString());
            assertEquals("PK2", primaryKeyColumns[1].getName());
            assertEquals(pk2Val, primaryKeyColumns[1].getValue().asLong());

            //assert column and originColumn equal
            List<RecordColumn> recordColumns = streamRecord.getColumns();
            List<RecordColumn> originRecordColumns = streamRecord.getOriginColumns();
            if (i == 0) {
                assertEquals(StreamRecord.RecordType.PUT, streamRecord.getRecordType());
                assertEquals(1, recordColumns.size());
                assertEquals(0, originRecordColumns.size());
                assertEquals("col1", recordColumns.get(0).getColumn().getName());
                assertEquals("col1ori", recordColumns.get(0).getColumn().getValue().asString());
            } else {
                assertEquals(StreamRecord.RecordType.UPDATE, streamRecord.getRecordType());
                assertEquals(1, recordColumns.size());
                assertEquals(1, originRecordColumns.size());
                assertEquals("col1", recordColumns.get(0).getColumn().getName());
                assertEquals("col1tar", recordColumns.get(0).getColumn().getValue().asString());
                assertEquals("col1", originRecordColumns.get(0).getColumn().getName());
                assertEquals("col1ori", originRecordColumns.get(0).getColumn().getValue().asString());
            }
        }
    }

    @Test
    public void testTunnelBackOff() throws Exception {
        // 1. Create Stream tunnel
        workerConfig.setMaxRetryIntervalInMillis(5000);
        workerConfig.setHeartbeatIntervalInSec(5);
        String tunnelId = createTunnel(tunnelClient, TunnelType.Stream);
        TunnelWorker tunnelWorker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            tunnelWorker.connectAndWorking();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long beginTs = System.currentTimeMillis();
        long count = consumeRound.get();
        int i = 0;
        for (; i < 100; i++) {
            if (consumeRound.get() - count > 0) {
                long cost = System.currentTimeMillis() - beginTs;
                beginTs = System.currentTimeMillis();
                if (cost >= 5000) {
                    System.out.println("Current round: " + i);
                    break;
                }
            }
            Thread.sleep(1000);
        }
        // Assume achieve max backoff
        assertTrue(i < 100);
    }
}
