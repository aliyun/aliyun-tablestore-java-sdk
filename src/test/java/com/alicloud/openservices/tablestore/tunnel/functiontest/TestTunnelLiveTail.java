package com.alicloud.openservices.tablestore.tunnel.functiontest;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RecordColumn.ColumnType;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelInfo;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelInfo;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;

import static org.junit.Assert.assertEquals;

public class TestTunnelLiveTail {
    private static final String Endpoint = "";
    private static final String AccessId = "";
    private static final String AccessKey = "";
    private static final String InstanceName = "";

    public static void main(String[] args) {
        TunnelClient tunnelClient = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName);
        SyncClient syncClient = new SyncClient(Endpoint, AccessId, AccessKey, InstanceName);

        String tableName = "test_zr" + System.currentTimeMillis();
        String tunnelName = "test_zr" + System.currentTimeMillis();

        try {
            //  1. create table
            System.out.println("Begin Create Table: " + tableName);
            createTable(syncClient, tableName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  2. create tunnel
            System.out.println("Begin Create Tunnel: " + tunnelName);
            createTunnel(tunnelClient, tableName, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  3. put data
            System.out.println("Begin Put Data");
            List<RowPutChange> expectChanges = putRows(syncClient, tableName);
            System.out.println("++++++++++++++++++++++++++++++++++++");
            System.out.println("Waiting............................");
            Thread.sleep(5000);

            //  4. describe tunnel
            System.out.println("Begin Describe Tunnel: " + tunnelName);
            DescribeTunnelResponse describeTunnelResponse = describeTunnel(tunnelClient, tableName, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  5. get checkpoint
            System.out.println("Begin GetCheckpoint");
            TunnelInfo tunnelInfo = describeTunnelResponse.getTunnelInfo();
            for (ChannelInfo channelInfo : describeTunnelResponse.getChannelInfos()) {
                GetCheckpointResponse getCheckpointResponse =
                    getcheckpoint(tunnelClient, tunnelInfo.getTunnelId(), channelInfo.getClientId(),
                        channelInfo.getChannelId());
                //  6. read some records
                System.out.println("Begin Read Records");
                ReadRecordsRequest request = new ReadRecordsRequest(tunnelInfo.getTunnelId(), channelInfo.getClientId(),
                    channelInfo.getChannelId(), getCheckpointResponse.getCheckpoint());
                ReadRecordsResponse resp = tunnelClient.readRecords(request);
                for (int count = 1; count < 5; count++) {
                    for (int i = 0; i < resp.getRecords().size(); i++) {
                        // check primarykey
                        StreamRecord record = resp.getRecords().get(i);
                        RowPutChange expectChange = expectChanges.get(i);
                        PrimaryKey pk = record.getPrimaryKey();
                        assertEquals(expectChange.getPrimaryKey().size(), pk.size());
                        assertEquals(expectChange.getPrimaryKey().getPrimaryKeyColumn(0), pk.getPrimaryKeyColumn(0));
                        assertEquals(expectChange.getPrimaryKey().getPrimaryKeyColumn(1), pk.getPrimaryKeyColumn(1));
                        // check column
                        assertEquals(expectChange.getColumnsToPut().size(), record.getColumns().size());
                        for (int j = 0; j < record.getColumns().size(); j++) {
                            assertEquals(ColumnType.PUT, record.getColumns().get(j).getColumnType());
                            assertEquals(expectChange.getColumnsToPut().get(j).getName(), record.getColumns().get(j)
                                .getColumn().getName());
                            assertEquals(expectChange.getColumnsToPut().get(j).getValue(), record.getColumns().get(j)
                                .getColumn().getValue());
                        }
                    }
                    System.out.println(resp.getRecords());
                    System.out.println(resp.getNextToken());
                    System.out.println("++++++++++++++++++++++++++++++++++++");
                    if (resp.getNextToken() == null) {
                        break;
                    }
                    request.setToken(resp.getNextToken());
                    Thread.sleep(2000);
                    resp = tunnelClient.readRecords(request);
                    System.out.println("Continue Read Records, Round: " + count);
                }

            }
            System.out.println("++++++++++++++++++++++++++++++++++++");

        } catch (TableStoreException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //  7. delete tunnel
            System.out.println("Begin Delete Tunnel: " + tunnelName);
            deleteTunnel(tunnelClient, tableName, tunnelName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            //  8. delete table
            System.out.println("Begin Delete Table: " + tableName);
            deleteTable(syncClient, tableName);
            System.out.println("++++++++++++++++++++++++++++++++++++");

            syncClient.shutdown();
            tunnelClient.shutdown();
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

    static List<RowPutChange> putRows(SyncClient client, String tableName) {
        List<RowPutChange> changes = new ArrayList<RowPutChange>();
        for (int i = 0; i < 1000; i++) {
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
        System.out.println("Put 1000 rows succeed.");
        return changes;
    }

    static void listTunnel(TunnelClient client, String tableName) {
        ListTunnelRequest request = new ListTunnelRequest(tableName);
        ListTunnelResponse resp = client.listTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        for (TunnelInfo info : resp.getTunnelInfos()) {
            System.out.println("TunnelInfo::::::");
            System.out.println("\tTunnelName: " + info.getTunnelName());
            System.out.println("\tTunnelId: " + info.getTunnelId());
            System.out.println("\tTunnelType: " + info.getTunnelType());
            System.out.println("\tTableName: " + info.getTableName());
            System.out.println("\tInstanceName: " + info.getInstanceName());
            System.out.println("\tStage: " + info.getStage());
            System.out.println("\tExpired: " + info.isExpired());
        }
    }

    static DescribeTunnelResponse describeTunnel(TunnelClient client, String tableName, String tunnelName) {
        DescribeTunnelRequest request = new DescribeTunnelRequest(tableName, tunnelName);
        DescribeTunnelResponse resp = client.describeTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
        System.out.println("TunnelRpo: " + resp.getTunnelRpo() + "ms");
        System.out.println("TunnelConsumePoint: " + resp.getTunnelConsumePoint());
        System.out.println("TunnelInfo::::::");
        TunnelInfo ti = resp.getTunnelInfo();
        System.out.println("\tTunnelName: " + ti.getTunnelName());
        System.out.println("\tTunnelId: " + ti.getTunnelId());
        System.out.println("\tTunnelType: " + ti.getTunnelType());
        System.out.println("\tTableName: " + ti.getTableName());
        System.out.println("\tInstanceName: " + ti.getInstanceName());
        System.out.println("\tStage: " + ti.getStage());
        System.out.println("\tExpired: " + ti.isExpired());
        System.out.println("ChannelInfos::::::");
        for (ChannelInfo ci : resp.getChannelInfos()) {
            System.out.println("\tChannelId: " + ci.getChannelId());
            System.out.println("\tChannelType: " + ci.getChannelType());
            System.out.println("\tClientId: " + ci.getClientId());
            System.out.println("\tChannelRpo: " + ci.getChannelRpo() + "ms");
            System.out.println("\tChannelConsumePoint: " + ci.getChannelConsumePoint());
            System.out.println("\tChannelCount: " + ci.getChannelCount());
        }
        return resp;
    }

    static void deleteTunnel(TunnelClient client, String tableName, String tunnelName) {
        DeleteTunnelRequest request = new DeleteTunnelRequest(tableName, tunnelName);
        DeleteTunnelResponse resp = client.deleteTunnel(request);
        System.out.println("RequestId: " + resp.getRequestId());
    }

    static GetCheckpointResponse getcheckpoint(TunnelClient client, String tunnelId, String clientId,
                                               String channelId) {
        GetCheckpointRequest request = new GetCheckpointRequest(tunnelId, clientId, channelId);
        GetCheckpointResponse resp = client.getCheckpoint(request);
        System.out.println("Checkpoint: " + resp.getCheckpoint());
        System.out.println("SequenceNumber: " + resp.getSequenceNumber());
        return resp;
    }
}
