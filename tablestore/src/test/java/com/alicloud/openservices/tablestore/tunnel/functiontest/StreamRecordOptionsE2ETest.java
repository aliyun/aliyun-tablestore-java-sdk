package com.alicloud.openservices.tablestore.tunnel.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RecordColumn;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.StreamColumn;
import com.alicloud.openservices.tablestore.model.StreamColumnType;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.StreamSpecification;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.StreamRecordOptions;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamRecordOptionsE2ETest {
    private static SyncClient syncClient;
    private static TunnelClient tunnelClient;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        syncClient = new SyncClient(
            settings.getOTSEndpoint(),
            settings.getOTSAccessKeyId(),
            settings.getOTSAccessKeySecret(),
            settings.getOTSInstanceName());
        tunnelClient = new TunnelClient(
            settings.getOTSEndpoint(),
            settings.getOTSAccessKeyId(),
            settings.getOTSAccessKeySecret(),
            settings.getOTSInstanceName());
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
    public void testTunnelDescribeAndConsumeLatestColumns() throws Exception {
        String tableName = "java_tunnel_old_new_" + System.currentTimeMillis();
        String tunnelName = "java_tunnel_" + System.currentTimeMillis();
        TunnelWorker tunnelWorker = null;
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, new TableOptions(-1, 1));
            StreamSpecification streamSpecification = new StreamSpecification(true, 168);
            streamSpecification.setOldColumnsToGet(new StreamColumn(StreamColumnType.ALL_COLUMNS, Arrays.<String>asList()));
            streamSpecification.setNewColumnsToGet(new StreamColumn(StreamColumnType.ALL_COLUMNS, Arrays.<String>asList()));
            createTableRequest.setStreamSpecification(streamSpecification);
            syncClient.createTable(createTableRequest);
            Thread.sleep(5000L);

            CreateTunnelRequest createTunnelRequest = new CreateTunnelRequest(tableName, tunnelName, TunnelType.Stream);
            StreamRecordOptions streamRecordOptions = new StreamRecordOptions();
            streamRecordOptions.setOldColumnsToGet(new StreamColumn(StreamColumnType.INPUT_COLUMNS, Arrays.<String>asList()));
            streamRecordOptions.setNewColumnsToGet(new StreamColumn(StreamColumnType.SPECIFIED_COLUMN, Arrays.asList("col1", "col2")));
            createTunnelRequest.setStreamRecordOptions(streamRecordOptions);
            CreateTunnelResponse createTunnelResponse = tunnelClient.createTunnel(createTunnelRequest);

            DescribeTunnelResponse describeTunnelResponse = tunnelClient.describeTunnel(new DescribeTunnelRequest(tableName, tunnelName));
            assertNotNull(describeTunnelResponse.getTunnelInfo().getStreamRecordOptions());
            assertNotNull(describeTunnelResponse.getTunnelInfo().getStreamRecordOptions().getOldColumnsToGet());
            assertNotNull(describeTunnelResponse.getTunnelInfo().getStreamRecordOptions().getNewColumnsToGet());

            final List<StreamRecord> consumedRecords = new CopyOnWriteArrayList<StreamRecord>();
            TunnelWorkerConfig config = new TunnelWorkerConfig(new IChannelProcessor() {
                @Override
                public void process(ProcessRecordsInput input) {
                    consumedRecords.addAll(input.getRecords());
                }

                @Override
                public void shutdown() {
                }
            });
            config.setHeartbeatIntervalInSec(5);
            tunnelWorker = new TunnelWorker(createTunnelResponse.getTunnelId(), tunnelClient, config);
            tunnelWorker.connectAndWorking();

            RowPutChange putRowChange = new RowPutChange(tableName,
                PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("pk1")).build());
            putRowChange.addColumn(new Column("col1", ColumnValue.fromString("before")));
            syncClient.putRow(new PutRowRequest(putRowChange));

            RowUpdateChange updateRowChange = new RowUpdateChange(tableName,
                PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("pk1")).build());
            updateRowChange.put(new Column("col1", ColumnValue.fromString("after")));
            updateRowChange.put(new Column("col2", ColumnValue.fromString("after2")));
            syncClient.updateRow(new UpdateRowRequest(updateRowChange));

            long deadline = System.currentTimeMillis() + 60000L;
            StreamRecord updateRecord = null;
            while (System.currentTimeMillis() < deadline) {
                for (StreamRecord record : consumedRecords) {
                    if (record.getRecordType() == StreamRecord.RecordType.UPDATE) {
                        updateRecord = record;
                        break;
                    }
                }
                if (updateRecord != null) {
                    break;
                }
                Thread.sleep(1000L);
            }

            assertNotNull(updateRecord);
            assertFalse(updateRecord.getOriginColumns().isEmpty());
            assertFalse(updateRecord.getLatestColumns().isEmpty());
            assertColumnValue(updateRecord.getOriginColumns(), "col1", "before");
            assertColumnValue(updateRecord.getLatestColumns(), "col1", "after");
            assertColumnValue(updateRecord.getLatestColumns(), "col2", "after2");
        } finally {
            if (tunnelWorker != null) {
                tunnelWorker.shutdown();
            }
            deleteTunnelIfExists(tableName, tunnelName);
            deleteTableIfExists(tableName);
        }
    }

    private static void assertColumnValue(List<RecordColumn> columns, String name, String value) {
        for (RecordColumn column : columns) {
            if (name.equals(column.getColumn().getName())) {
                assertEquals(value, column.getColumn().getValue().asString());
                return;
            }
        }
        assertTrue("column not found: " + name, false);
    }

    private static void deleteTunnelIfExists(String tableName, String tunnelName) {
        try {
            tunnelClient.deleteTunnel(new DeleteTunnelRequest(tableName, tunnelName));
        } catch (TableStoreException ignored) {
        }
    }

    private static void deleteTableIfExists(String tableName) {
        try {
            syncClient.deleteTable(new DeleteTableRequest(tableName));
        } catch (TableStoreException ignored) {
        }
    }
}
