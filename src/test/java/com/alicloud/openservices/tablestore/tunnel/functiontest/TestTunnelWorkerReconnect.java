package com.alicloud.openservices.tablestore.tunnel.functiontest;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTunnelWorkerReconnect {
    private static final String CONF_PATH = "tunnel.config";
    private static String endpoint = "";
    private static String accessId = "";
    private static String accessKey = "";
    private static String instanceName = "";

    @BeforeClass
    public static  void loadConfig() {
        System.out.println("here");
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(CONF_PATH));
            endpoint = properties.getProperty("OtsEndpoint");
            accessId = properties.getProperty("AccessId");
            accessKey = properties.getProperty("AccessKey");
            instanceName = properties.getProperty("InstanceName");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class SimpleProcessor implements IChannelProcessor {
        @Override
        public void process(ProcessRecordsInput input) {
            System.out.println("Default record processor, would print records count");
            System.out.println(
                String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));
            try {
                // Mock Record Process.
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void shutdown() {
            System.out.println("Mock shutdown");
        }
    }

    static void createTunnel(TunnelClient client, String tableName, String tunnelName) {
        CreateTunnelRequest request = new CreateTunnelRequest(tableName, tunnelName, TunnelType.Stream);
        CreateTunnelResponse resp = client.createTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        System.out.println("TunnelId: " + resp.getTunnelId());
    }

    static void createTable(SyncClient client, String tableName) {
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumn("int", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("str", PrimaryKeyType.STRING);
        CreateTableRequest request = new CreateTableRequest(meta, new TableOptions(-1, 1));
        client.createTable(request);

    }

    static void deleteTable(SyncClient client, String tableName) {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        client.deleteTable(request);
    }

    static List<RowPutChange> putRows(SyncClient client, String tableName, int rowCount) {
        List<RowPutChange> changes = new ArrayList<RowPutChange>();
        for (int i = 0; i < rowCount; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("int", PrimaryKeyValue.fromLong(i));
            primaryKeyBuilder.addPrimaryKeyColumn("str", PrimaryKeyValue.fromString("string" + i));
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKeyBuilder.build());

            for (int j = 0; j < 10; j++) {
                rowPutChange.addColumn(new Column("test" + j, ColumnValue.fromLong(i)));
            }
            client.putRow(new PutRowRequest(rowPutChange));
            changes.add(rowPutChange);
        }
        System.out.println(String.format("Put %d rows succeed.", rowCount));
        return changes;
    }

    static void deleteTunnel(TunnelClient client, String tableName, String tunnelName) {
        DeleteTunnelRequest request = new DeleteTunnelRequest(tableName, tunnelName);
        DeleteTunnelResponse resp = client.deleteTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
    }

    @Test
    public void testTunnelWorkerReconnect_WithTunnelInvalid() {
        TunnelClient tunnelClient = new TunnelClient(endpoint, accessId, accessKey, instanceName);
        SyncClient syncClient = new SyncClient(endpoint, accessId, accessKey, instanceName);

        String tableName = "test_zr" + System.currentTimeMillis();
        String tunnelName = "test_zr" + System.currentTimeMillis();
        //  1. create table
        System.out.println("Begin Create Table: " + tableName);
        createTable(syncClient, tableName);
        System.out.println("++++++++++++++++++++++++++++++++++++");

        //  2. create tunnel
        System.out.println("Begin Create Tunnel: " + tunnelName);
        CreateTunnelResponse resp = tunnelClient.createTunnel(
            new CreateTunnelRequest(tableName, tunnelName, TunnelType.Stream));
        String tunnelId = resp.getTunnelId();
        System.out.println("Create Tunnel, Id: " + tunnelId);
        System.out.println("++++++++++++++++++++++++++++++++++++");

        //  3. put data
        System.out.println("Begin Put Data in backend.");
        putRows(syncClient, tableName, 5000);
        System.out.println("++++++++++++++++++++++++++++++++++++");

        // 4. new tunnel worker and consume.
        TunnelWorkerConfig config = new TunnelWorkerConfig(new SimpleProcessor());
        TunnelWorker worker = new TunnelWorker(tunnelId, tunnelClient, config);
        try {
            worker.connectAndWorking();
            Thread.sleep(50000);
            System.out.println("Begin Delete Tunnel: " + tunnelName);
            deleteTunnel(tunnelClient, tableName, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");
            Thread.sleep(50000);
            // cannot achieve here.
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            worker.shutdown();
        } finally {
            //  delete table
            System.out.println("Begin Delete Table: " + tableName);
            deleteTable(syncClient, tableName);
            System.out.println("++++++++++++++++++++++++++++++++++++");
            worker.shutdown();
            syncClient.shutdown();
            tunnelClient.shutdown();
        }
    }

}
