package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeStreamRequest;
import com.alicloud.openservices.tablestore.model.GetShardIteratorRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RecordColumn;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.alicloud.openservices.tablestore.model.Stream;
import com.alicloud.openservices.tablestore.model.StreamColumn;
import com.alicloud.openservices.tablestore.model.StreamColumnType;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.StreamShard;
import com.alicloud.openservices.tablestore.model.StreamSpecification;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamOldNewColumnsTest {
    private static SyncClient client;

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
    public void testGetStreamRecordReturnsOriginAndLatestColumns() throws Exception {
        String tableName = "java_stream_old_new_" + System.currentTimeMillis();
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));
            CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta, new TableOptions(-1, 1));
            StreamSpecification streamSpecification = new StreamSpecification(true, 168);
            streamSpecification.setOldColumnsToGet(new StreamColumn(StreamColumnType.ALL_COLUMNS, Arrays.<String>asList()));
            streamSpecification.setNewColumnsToGet(new StreamColumn(StreamColumnType.ALL_COLUMNS, Arrays.<String>asList()));
            createTableRequest.setStreamSpecification(streamSpecification);
            client.createTable(createTableRequest);
            Thread.sleep(5000L);

            RowPutChange putRowChange = new RowPutChange(tableName,
                PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("pk1")).build());
            putRowChange.addColumn(new Column("col1", ColumnValue.fromString("before")));
            client.putRow(new PutRowRequest(putRowChange));

            RowUpdateChange updateRowChange = new RowUpdateChange(tableName,
                PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("pk1")).build());
            updateRowChange.put(new Column("col1", ColumnValue.fromString("after")));
            updateRowChange.put(new Column("col2", ColumnValue.fromString("after2")));
            client.updateRow(new UpdateRowRequest(updateRowChange));

            Thread.sleep(3000L);

            List<Stream> streams = client.listStream(new ListStreamRequest(tableName)).getStreams();
            assertFalse(streams.isEmpty());
            String streamId = streams.get(0).getStreamId();

            List<StreamShard> shards = client.describeStream(new DescribeStreamRequest(streamId)).getShards();
            assertFalse(shards.isEmpty());

            String shardIterator = client.getShardIterator(new GetShardIteratorRequest(streamId, shards.get(0).getShardId())).getShardIterator();
            GetStreamRecordRequest getStreamRecordRequest = new GetStreamRecordRequest(shardIterator);
            getStreamRecordRequest.setTableName(tableName);
            getStreamRecordRequest.setOldColumnsToGet(new StreamColumn(StreamColumnType.INPUT_COLUMNS, Arrays.<String>asList()));
            getStreamRecordRequest.setNewColumnsToGet(new StreamColumn(StreamColumnType.SPECIFIED_COLUMN, Arrays.asList("col1", "col2")));

            GetStreamRecordResponse response = client.getStreamRecord(getStreamRecordRequest);
            assertNotNull(response);
            assertFalse(response.getRecords().isEmpty());

            StreamRecord updateRecord = null;
            for (StreamRecord record : response.getRecords()) {
                if (record.getRecordType() == StreamRecord.RecordType.UPDATE) {
                    updateRecord = record;
                    break;
                }
            }
            assertNotNull(updateRecord);
            assertFalse(updateRecord.getOriginColumns().isEmpty());
            assertFalse(updateRecord.getLatestColumns().isEmpty());
            assertColumnValue(updateRecord.getOriginColumns(), "col1", "before");
            assertColumnValue(updateRecord.getLatestColumns(), "col1", "after");
            assertColumnValue(updateRecord.getLatestColumns(), "col2", "after2");
        } finally {
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

    private static void deleteTableIfExists(String tableName) {
        try {
            client.deleteTable(new DeleteTableRequest(tableName));
        } catch (TableStoreException ignored) {
        }
    }
}
