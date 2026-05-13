package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.GetStreamRecordRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.StreamColumn;
import com.alicloud.openservices.tablestore.model.StreamColumnType;
import com.alicloud.openservices.tablestore.model.StreamDetails;
import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.model.StreamSpecification;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.StreamRecordOptions;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

public class StreamOldNewColumnsProtocolTest {

    @Test
    public void testBuildStreamSpecificationAndGetStreamRecordRequestWithOldNewColumns() {
        StreamSpecification specification = new StreamSpecification(true, 168);
        specification.setOldColumnsToGet(new StreamColumn(StreamColumnType.SPECIFIED_COLUMN, Arrays.asList("old1", "old2")));
        specification.setNewColumnsToGet(new StreamColumn(StreamColumnType.ALL_COLUMNS, Collections.<String>emptyList()));
        TableMeta tableMeta = new TableMeta("table");
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));

        OtsInternalApi.CreateTableRequest pbCreateTableRequest = OTSProtocolBuilder.buildCreateTableRequest(
            new com.alicloud.openservices.tablestore.model.CreateTableRequest(
                tableMeta,
                new com.alicloud.openservices.tablestore.model.TableOptions(-1, 1)) {{
                    setStreamSpecification(specification);
                }});
        OtsInternalApi.StreamSpecification pbSpecification = pbCreateTableRequest.getStreamSpec();
        assertEquals(OtsInternalApi.StreamColumnType.SPECIFIED_COLUMN, pbSpecification.getOldColumnsToGet().getType());
        assertEquals(Arrays.asList("old1", "old2"), pbSpecification.getOldColumnsToGet().getColumnNameList());
        assertEquals(OtsInternalApi.StreamColumnType.ALL_COLUMNS, pbSpecification.getNewColumnsToGet().getType());

        GetStreamRecordRequest request = new GetStreamRecordRequest("iterator");
        request.setOldColumnsToGet(new StreamColumn(StreamColumnType.INPUT_COLUMNS, Collections.<String>emptyList()));
        request.setNewColumnsToGet(new StreamColumn(StreamColumnType.SPECIFIED_COLUMN, Arrays.asList("new1")));

        OtsInternalApi.GetStreamRecordRequest pbRequest = OTSProtocolBuilder.buildGetStreamRecordRequest(request);
        assertEquals(OtsInternalApi.StreamColumnType.INPUT_COLUMNS, pbRequest.getOldColumnsToGet().getType());
        assertEquals(OtsInternalApi.StreamColumnType.SPECIFIED_COLUMN, pbRequest.getNewColumnsToGet().getType());
        assertEquals(Collections.singletonList("new1"), pbRequest.getNewColumnsToGet().getColumnNameList());
    }

    @Test
    public void testParseLatestColumnsForStreamAndTunnelResponses() throws Exception {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(1L))
            .build();
        long ts = 123L;

        ByteString recordBytes = ByteString.copyFrom(buildRow(primaryKey, new Column("col", ColumnValue.fromString("new"), ts)));
        ByteString originBytes = ByteString.copyFrom(buildRow(primaryKey, new Column("col", ColumnValue.fromString("old"), ts)));
        ByteString latestBytes = ByteString.copyFrom(buildRowWithoutPk(new Column("col", ColumnValue.fromString("latest"), ts)));

        OtsInternalApi.GetStreamRecordResponse.StreamRecord pbRecord =
            OtsInternalApi.GetStreamRecordResponse.StreamRecord.newBuilder()
                .setActionType(OtsInternalApi.ActionType.UPDATE_ROW)
                .setRecord(recordBytes)
                .setOriginRecord(originBytes)
                .setLatestColumns(latestBytes)
                .build();

        GetStreamRecordResponse response = ResponseFactory.createGetStreamRecordResponse(
            new ResponseContentWithMeta(null, new com.alicloud.openservices.tablestore.model.Response()),
            OtsInternalApi.GetStreamRecordResponse.newBuilder().addStreamRecords(pbRecord).build(),
            false);

        StreamRecord streamRecord = response.getRecords().get(0);
        assertEquals(1, streamRecord.getOriginColumns().size());
        assertEquals(1, streamRecord.getLatestColumns().size());
        assertEquals("latest", streamRecord.getLatestColumns().get(0).getColumn().getValue().asString());

        TunnelServiceApi.Record tunnelRecord = TunnelServiceApi.Record.newBuilder()
            .setActionType(TunnelServiceApi.ActionType.UPDATE_ROW)
            .setRecord(recordBytes)
            .setOriginRecord(ByteString.copyFrom(buildRowWithoutPk(new Column("col", ColumnValue.fromString("old"), ts))))
            .setLatestRecord(latestBytes)
            .build();

        com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse tunnelResponse =
            ResponseFactory.createReadRecordsResponse(
                new ResponseContentWithMeta(null, new com.alicloud.openservices.tablestore.model.Response()),
                TunnelServiceApi.ReadRecordsResponse.newBuilder().addRecords(tunnelRecord).setNextToken("token").build());

        StreamRecord tunnelStreamRecord = tunnelResponse.getRecords().get(0);
        assertEquals(1, tunnelStreamRecord.getOriginColumns().size());
        assertEquals(1, tunnelStreamRecord.getLatestColumns().size());
        assertEquals("latest", tunnelStreamRecord.getLatestColumns().get(0).getColumn().getValue().asString());
    }

    @Test
    public void testBuildTunnelStreamRecordOptions() {
        CreateTunnelRequest request = new CreateTunnelRequest("table", "tunnel", TunnelType.Stream);
        StreamRecordOptions options = new StreamRecordOptions();
        options.setOldColumnsToGet(new StreamColumn(StreamColumnType.INPUT_COLUMNS, Collections.<String>emptyList()));
        options.setNewColumnsToGet(new StreamColumn(StreamColumnType.SPECIFIED_COLUMN, Arrays.asList("col5", "col6")));
        request.setStreamRecordOptions(options);

        TunnelServiceApi.CreateTunnelRequest pbRequest = TunnelProtocolBuilder.buildCreateTunnelRequest(request);
        assertNotNull(pbRequest.getTunnel().getStreamRecordOptions());
        assertEquals(TunnelServiceApi.StreamColumnType.INPUT_COLUMNS,
            pbRequest.getTunnel().getStreamRecordOptions().getOldColumnsToGet().getType());
        assertEquals(TunnelServiceApi.StreamColumnType.SPECIFIED_COLUMN,
            pbRequest.getTunnel().getStreamRecordOptions().getNewColumnsToGet().getType());
        assertEquals(Arrays.asList("col5", "col6"),
            pbRequest.getTunnel().getStreamRecordOptions().getNewColumnsToGet().getColumnNameList());
    }

    @Test
    public void testParseStreamColumnInvalidPlaceholderReturnsNull() {
        OtsInternalApi.StreamColumn placeholder = OtsInternalApi.StreamColumn.newBuilder()
            .setType(OtsInternalApi.StreamColumnType.INVALID)
            .build();

        assertNull(OTSProtocolParser.parseStreamColumn(placeholder));
    }

    @Test
    public void testDescribeTableParsesPlaceholderStreamColumnsAsUnset() {
        OtsInternalApi.StreamColumn placeholder = OtsInternalApi.StreamColumn.newBuilder()
            .setType(OtsInternalApi.StreamColumnType.INVALID)
            .build();

        OtsInternalApi.DescribeTableResponse pbResponse = OtsInternalApi.DescribeTableResponse.newBuilder()
            .setTableMeta(OtsInternalApi.TableMeta.newBuilder()
                .setTableName("table")
                .addPrimaryKey(OtsInternalApi.PrimaryKeySchema.newBuilder()
                    .setName("pk")
                    .setType(OtsInternalApi.PrimaryKeyType.INTEGER)))
            .setTableOptions(OtsInternalApi.TableOptions.newBuilder()
                .setTimeToLive(-1)
                .setMaxVersions(1))
            .setReservedThroughputDetails(OtsInternalApi.ReservedThroughputDetails.newBuilder()
                .setCapacityUnit(OtsInternalApi.CapacityUnit.newBuilder().setRead(0).setWrite(0))
                .setLastIncreaseTime(0))
            .setStreamDetails(OtsInternalApi.StreamDetails.newBuilder()
                .setEnableStream(true)
                .setExpirationTime(168)
                .setOldColumnsToGet(placeholder)
                .setNewColumnsToGet(placeholder))
            .build();

        StreamDetails streamDetails = ResponseFactory.createDescribeTableResponse(
            new ResponseContentWithMeta(null, new com.alicloud.openservices.tablestore.model.Response()),
            pbResponse).getStreamDetails();

        assertNull(streamDetails.getOldColumnsToGet());
        assertNull(streamDetails.getNewColumnsToGet());
    }

    @Test
    public void testDescribeTunnelParsesPlaceholderStreamColumnsAsUnset() {
        TunnelServiceApi.StreamColumn placeholder = TunnelServiceApi.StreamColumn.newBuilder()
            .setType(TunnelServiceApi.StreamColumnType.INVALID)
            .build();

        TunnelServiceApi.DescribeTunnelResponse pbResponse = TunnelServiceApi.DescribeTunnelResponse.newBuilder()
            .setTunnel(TunnelServiceApi.TunnelInfo.newBuilder()
                .setTunnelId("tunnel-id")
                .setTunnelType(TunnelType.Stream.name())
                .setTableName("table")
                .setInstanceName("instance")
                .setStreamId("stream-id")
                .setStage("ProcessStream")
                .setCreateTime(0)
                .setStreamRecordOptions(TunnelServiceApi.StreamRecordOptions.newBuilder()
                    .setOldColumnsToGet(placeholder)
                    .setNewColumnsToGet(placeholder)))
            .build();

        DescribeTunnelResponse response = ResponseFactory.createDescribeTunnelResponse(
            new ResponseContentWithMeta(null, new com.alicloud.openservices.tablestore.model.Response()),
            pbResponse);

        assertNotNull(response.getTunnelInfo().getStreamRecordOptions());
        assertNull(response.getTunnelInfo().getStreamRecordOptions().getOldColumnsToGet());
        assertNull(response.getTunnelInfo().getStreamRecordOptions().getNewColumnsToGet());
    }

    private static byte[] buildRow(PrimaryKey primaryKey, Column... columns) throws Exception {
        com.alicloud.openservices.tablestore.model.RowPutChange rowPutChange =
            new com.alicloud.openservices.tablestore.model.RowPutChange("table", primaryKey);
        for (Column column : columns) {
            rowPutChange.addColumn(column);
        }
        return PlainBufferBuilder.buildRowPutChangeWithHeader(rowPutChange);
    }

    private static byte[] buildRowWithoutPk(Column... columns) throws Exception {
        java.util.List<PlainBufferCell> cells = new java.util.ArrayList<PlainBufferCell>();
        for (Column column : columns) {
            cells.add(PlainBufferConversion.toPlainBufferCell(column, false, false, false, (byte) 0x0));
        }
        PlainBufferRow row = new PlainBufferRow(java.util.Collections.<PlainBufferCell>emptyList(), cells, false);
        int size = PlainBufferBuilder.computePlainBufferRowWithHeader(row);
        PlainBufferOutputStream output = new PlainBufferOutputStream(size);
        PlainBufferCodedOutputStream codedOutput = new PlainBufferCodedOutputStream(output);
        codedOutput.writeRowWithHeader(row);
        return output.getBuffer();
    }
}
