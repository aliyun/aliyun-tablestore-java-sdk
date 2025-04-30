package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TimeseriesStreamTest {

    static String testTable = "TestTimeseriesStream";

    static SyncClient client = null;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        prepareTimeseriesTable();
    }

    @AfterClass
    public static void afterClass() {
        client.shutdown();
    }

    private static void prepareTimeseriesTable() {
        /**
         * If there is an old time-series table, delete it.
         */
        try {
            client.asTimeseriesClient().deleteTimeseriesTable(new DeleteTimeseriesTableRequest(testTable));
        } catch (Exception e) {
        }
        /**
         * Create a new time-series table and write data
         */
        try {
            TimeseriesTableMeta meta = new TimeseriesTableMeta(testTable);
            CreateTimeseriesTableRequest createTableRequest = new CreateTimeseriesTableRequest(meta);

            client.asTimeseriesClient().createTimeseriesTable(createTableRequest);
            System.out.println("Waiting for creating Timeseries Table...");
            Thread.sleep(60000);
            System.out.println("Table created successfully.");

            PutTimeseriesDataRequest putTimeseriesDataRequest = new PutTimeseriesDataRequest(testTable);
            for (int i = 0; i < 10; i++) {
                Map<String, String> tags = new HashMap<String, String>();
                tags.put("tag" + i + "_1", "v" + i + "_1");
                tags.put("tag" + i + "_2", "v" + i + "_2");
                TimeseriesKey key = new TimeseriesKey("measurementName" + i, "dataSource" + i, tags);
                TimeseriesRow row = new TimeseriesRow(key, System.currentTimeMillis() * 1000);
                row.addField("time", ColumnValue.fromString(new Date().toString()));
                putTimeseriesDataRequest.addRow(row);
            }
            client.asTimeseriesClient().putTimeseriesData(putTimeseriesDataRequest);

        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    private static String getShardIteratorFromShardId(String streamId, String shardId) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest(streamId, shardId);
        GetShardIteratorResponse getShardIteratorResponse = client.getShardIterator(getShardIteratorRequest);
        return getShardIteratorResponse.getShardIterator();
    }

    private static List<StreamRecord> getStreamRecordFromShardIterator(String shardIterator) {
        GetStreamRecordRequest getStreamRecordRequest = new GetStreamRecordRequest(shardIterator);
        getStreamRecordRequest.setParseInTimeseriesDataFormat(true);
        GetStreamRecordResponse getStreamRecordResponse = client.getStreamRecord(getStreamRecordRequest);
        return getStreamRecordResponse.getRecords();
    }

    @Test
    public void testGetTimeseriesStream() {
        ListStreamRequest request = new ListStreamRequest();
        ListStreamResponse response = client.listStream(request);
        String streamId = null;
        for (Stream stream : response.getStreams()) {
            if (stream.getTableName().equals(testTable)) {
                streamId = stream.getStreamId();
                break;
            }
        }
        if (streamId == null) {
            fail("Did not obtained streamId.");
        }
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(streamId);
        describeStreamRequest.setSupportTimeseriesTable(true);
        DescribeStreamResponse describeStreamResponse = client.describeStream(describeStreamRequest);

        int totalSize = 0;
        for (StreamShard s : describeStreamResponse.getShards()
        ) {
            String shardIterator = getShardIteratorFromShardId(streamId, s.getShardId());
            List<StreamRecord> streamRecordList = getStreamRecordFromShardIterator(shardIterator);
            totalSize += streamRecordList.size();
            for (StreamRecord streamRecord : streamRecordList) {
                assertEquals(streamRecord.getRecordType(), StreamRecord.RecordType.PUT);
                assertEquals(streamRecord.getPrimaryKey().getPrimaryKeyColumn(0).getName(), "_m_name");
                assertEquals(streamRecord.getColumns().get(0).getColumn().getName(), "time");

            }
        }
        assertEquals(totalSize, 10);
    }

    @Test
    public void testGetTimeseriesStreamWithoutSetSupportTimeseriesTable() {
        ListStreamRequest request = new ListStreamRequest();
        ListStreamResponse response = client.listStream(request);
        String streamId = null;
        for (Stream stream : response.getStreams()) {
            if (stream.getTableName().equals(testTable)) {
                streamId = stream.getStreamId();
                break;
            }
        }
        if (streamId == null) {
            fail("Did not obtained streamId.");
        }
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest(streamId);
        try {
            client.describeStream(describeStreamRequest);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Table [" + testTable + "] is a timeseries table, not support DescribeStream");
        }
    }


}
