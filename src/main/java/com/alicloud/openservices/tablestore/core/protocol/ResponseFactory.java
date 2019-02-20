package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi.ComputeSplitPointsBySizeResponse.SplitLocation;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.Split;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelInfo;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelType;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelInfo;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelStage;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelResponse;
import com.google.protobuf.ByteString;

public class ResponseFactory {

    public static CreateTableResponse createCreateTableResponse(ResponseContentWithMeta response,
                                                                OtsInternalApi.CreateTableResponse
                                                                    createTableResponse) {
        return new CreateTableResponse(response.getMeta());
    }

    public static ListTableResponse createListTableResponse(ResponseContentWithMeta response,
                                                            OtsInternalApi.ListTableResponse listTableResponse) {
        ListTableResponse result = new ListTableResponse(response.getMeta());
        result.setTableNames(listTableResponse.getTableNamesList());
        return result;
    }

    public static DescribeTableResponse createDescribeTableResponse(ResponseContentWithMeta response,
                                                                    OtsInternalApi.DescribeTableResponse
                                                                        describeTableResponse) {
        try {
            DescribeTableResponse result = new DescribeTableResponse(response.getMeta());
            result.setTableMeta(OTSProtocolParser.parseTableMeta(describeTableResponse.getTableMeta()));
            result.setTableOptions(OTSProtocolParser.parseTableOptions(describeTableResponse.getTableOptions()));
            result.setReservedThroughputDetails(OTSProtocolParser
                    .parseReservedThroughputDetails(describeTableResponse.getReservedThroughputDetails()));
            if (describeTableResponse.hasStreamDetails()) {
                result.setStreamDetails(OTSProtocolParser.parseStreamDetails(describeTableResponse.getStreamDetails()));
            }

            List<PrimaryKey> shards = new ArrayList<PrimaryKey>();
            for (int i = 0; i < describeTableResponse.getShardSplitsCount(); ++i) {
                com.google.protobuf.ByteString bytes = describeTableResponse.getShardSplits(i);
                PlainBufferCodedInputStream coded = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(bytes.asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = coded.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                Row row = PlainBufferConversion.toRow(rows.get(0));
                shards.add(row.getPrimaryKey());
            }
            result.setShardSplits(shards);
            for (int i = 0; i < describeTableResponse.getIndexMetasCount(); ++i) {
                result.addIndexMeta(OTSProtocolParser.parseIndexMeta(describeTableResponse.getIndexMetas(i)));
            }
            return result;
        } catch (Exception e) {
            throw new ClientException("Failed to parse describe table response.", e);
        }
    }

    public static DeleteTableResponse createDeleteTableResponse(ResponseContentWithMeta response,
                                                                OtsInternalApi.DeleteTableResponse
                                                                    deleteTableResponse) {
        return new DeleteTableResponse(response.getMeta());
    }

    public static CreateIndexResponse createCreteIndexResponse(
        ResponseContentWithMeta response,
        OtsInternalApi.CreateIndexResponse createIndexResponse) {
        return new CreateIndexResponse(response.getMeta());
    }

    public static DeleteIndexResponse createDeleteIndexResponse(
        ResponseContentWithMeta response,
        OtsInternalApi.DropIndexResponse dropIndexResponse) {
        return new DeleteIndexResponse(response.getMeta());
    }

    public static UpdateTableResponse createUpdateTableResponse(ResponseContentWithMeta response,
                                                                OtsInternalApi.UpdateTableResponse
                                                                    updateTableResponse) {
        UpdateTableResponse result = new UpdateTableResponse(response.getMeta());
        result.setReservedThroughputDetails(
                OTSProtocolParser.parseReservedThroughputDetails(updateTableResponse.getReservedThroughputDetails()));

        if (updateTableResponse.hasStreamDetails()) {
            result.setStreamDetails(OTSProtocolParser.parseStreamDetails(updateTableResponse.getStreamDetails()));
        }
        return result;
    }

    public static GetRowResponse createGetRowResponse(ResponseContentWithMeta response,
                                                      OtsInternalApi.GetRowResponse getRowResponse) {
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(
            OTSProtocolParser.parseCapacityUnit(getRowResponse.getConsumed().getCapacityUnit()));
        Row row = null;
        if (!getRowResponse.getRow().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(getRowResponse.getRow().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                row = PlainBufferConversion.toRow(rows.get(0));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }
        GetRowResponse result = new GetRowResponse(response.getMeta(), row, consumedCapacity);
        if (getRowResponse.hasNextToken()) {
            result.setNextToken(getRowResponse.getNextToken().toByteArray());
        }
        return result;
    }

    public static PutRowResponse createPutRowResponse(ResponseContentWithMeta response,
                                                      OtsInternalApi.PutRowResponse putRowResponse) {
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(
            OTSProtocolParser.parseCapacityUnit(putRowResponse.getConsumed().getCapacityUnit()));
        Row row = null;
        if (!putRowResponse.getRow().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(putRowResponse.getRow().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                row = PlainBufferConversion.toRow(rows.get(0));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }

        PutRowResponse result = new PutRowResponse(response.getMeta(), row, consumedCapacity);
        return result;
    }

    public static UpdateRowResponse createUpdateRowResponse(ResponseContentWithMeta response,
                                                            OtsInternalApi.UpdateRowResponse updateRowResponse) {
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(
            OTSProtocolParser.parseCapacityUnit(updateRowResponse.getConsumed().getCapacityUnit()));
        Row row = null;
        if (!updateRowResponse.getRow().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(updateRowResponse.getRow().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                row = PlainBufferConversion.toRow(rows.get(0));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }

        UpdateRowResponse result = new UpdateRowResponse(response.getMeta(), row, consumedCapacity);
        return result;
    }

    public static DeleteRowResponse createDeleteRowResponse(ResponseContentWithMeta response,
                                                            OtsInternalApi.DeleteRowResponse deleteRowResponse) {
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(
            OTSProtocolParser.parseCapacityUnit(deleteRowResponse.getConsumed().getCapacityUnit()));
        Row row = null;
        if (!deleteRowResponse.getRow().isEmpty()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(deleteRowResponse.getRow().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                row = PlainBufferConversion.toRow(rows.get(0));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }

        DeleteRowResponse result = new DeleteRowResponse(response.getMeta(), row, consumedCapacity);
        return result;
    }

    public static GetRangeResponse createGetRangeResponse(ResponseContentWithMeta response,
                                                          OtsInternalApi.GetRangeResponse getRangeResponse) {
        try {
            ConsumedCapacity consumedCapacity = new ConsumedCapacity(
                OTSProtocolParser.parseCapacityUnit(getRangeResponse.getConsumed().getCapacityUnit()));
            GetRangeResponse result = new GetRangeResponse(response.getMeta(), consumedCapacity);

            if (!getRangeResponse.hasNextStartPrimaryKey()) {
                // has no next primary key
                result.setNextStartPrimaryKey(null);
            } else {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(getRangeResponse.getNextStartPrimaryKey().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                PlainBufferRow row = rows.get(0);
                if (row.hasDeleteMarker() || row.hasCells()) {
                    throw new IOException("The next primary key should only have primary key: " + row);
                }

                result.setNextStartPrimaryKey(PlainBufferConversion.toPrimaryKey(row.getPrimaryKey()));
            }

            if (!getRangeResponse.getRows().isEmpty()) {
                List<Row> rows = new ArrayList<Row>();
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                        new PlainBufferInputStream(getRangeResponse.getRows().asReadOnlyByteBuffer()));
                List<PlainBufferRow> pbRows = inputStream.readRowsWithHeader();
                for (PlainBufferRow pbRow : pbRows) {
                    rows.add(PlainBufferConversion.toRow(pbRow));
                }
                result.setRows(rows);
            } else {
                result.setRows(new ArrayList<Row>());
            }

            if (getRangeResponse.hasNextToken()) {
                result.setNextToken(getRangeResponse.getNextToken().toByteArray());
            }

            return result;
        } catch (Exception e) {
            throw new ClientException("Failed to parse get range response.", e);
        }
    }

    public static ComputeSplitsBySizeResponse createComputeSplitsBySizeResponse(ResponseContentWithMeta response,
                                                                                OtsInternalApi
                                                                                    .ComputeSplitPointsBySizeResponse
                                                                                    computeSplitPointsBySizeResponse) {
        ComputeSplitsBySizeResponse result = new ComputeSplitsBySizeResponse(response.getMeta());

        ConsumedCapacity consumedCapacity = new ConsumedCapacity(
            OTSProtocolParser.parseCapacityUnit(computeSplitPointsBySizeResponse.getConsumed().getCapacityUnit()));
        result.setConsumedCapacity(consumedCapacity);

        List<com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi.PrimaryKeySchema> schemaList
            = computeSplitPointsBySizeResponse
            .getSchemaList();
        for (com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi.PrimaryKeySchema pks : schemaList) {
            if (pks.hasOption()) {
                result.addPrimaryKeySchema(pks.getName(), OTSProtocolParser.toPrimaryKeyType(pks.getType()),
                    OTSProtocolParser.toPrimaryKeyOption(pks.getOption()));
            } else {
                result.addPrimaryKeySchema(pks.getName(), OTSProtocolParser.toPrimaryKeyType(pks.getType()));
            }
        }

        List<PrimaryKeyColumn> infStartColumns = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> infEndColumns = new ArrayList<PrimaryKeyColumn>();
        for (PrimaryKeySchema pks : result.getPrimaryKeySchema()) {
            infEndColumns.add(new PrimaryKeyColumn(pks.getName(), PrimaryKeyValue.INF_MAX));
            infStartColumns.add(new PrimaryKeyColumn(pks.getName(), PrimaryKeyValue.INF_MIN));
        }
        PrimaryKey infStart = new PrimaryKey(infStartColumns);
        PrimaryKey infEnd = new PrimaryKey(infEndColumns);
        PrimaryKey lastStartPoint = infStart;

        for (int iter = 0; iter < computeSplitPointsBySizeResponse.getSplitPointsCount(); ++iter) {
            Split split = new Split();
            split.setLowerBound(lastStartPoint);

            Row row = null;
            try {
                ByteString bss = computeSplitPointsBySizeResponse.getSplitPoints(iter);
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(bss.asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() <= 0) {
                    throw new ClientException("The parsed response rows' length is zero.");
                }
                row = PlainBufferConversion.toRow(rows.get(0));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row data.", e);
            }
            PrimaryKey primaryKeys = row.getPrimaryKey();
            List<PrimaryKeyColumn> pkcl = new ArrayList<PrimaryKeyColumn>();
            for (PrimaryKeyColumn pkcc : primaryKeys.getPrimaryKeyColumns()) {
                pkcl.add(pkcc);
            }
            for (int loc = pkcl.size(); loc < result.getPrimaryKeySchema().size(); ++loc) {
                pkcl.add(new PrimaryKeyColumn(result.getPrimaryKeySchema().get(loc).getName(),
                    PrimaryKeyValue.INF_MIN));
            }
            primaryKeys = new PrimaryKey(pkcl);
            split.setUpperBound(primaryKeys);
            lastStartPoint = primaryKeys;

            result.addSplit(split);
        }
        // To insert the last split element.
        Split split = new Split();
        split.setLowerBound(lastStartPoint);
        split.setUpperBound(infEnd);
        result.addSplit(split);

        int splitPointIter = 0;
        for (SplitLocation sl : computeSplitPointsBySizeResponse.getLocationsList()) {
            for (long sli = 0; sli < sl.getRepeat(); ++sli) {
                if (splitPointIter >= result.getSplits().size()) {
                    throw new ClientException(
                        "The location list's length is not correct. Location list size: " + splitPointIter
                            + ". Split list size: " + result.getSplits().size());
                }
                result.getSplits().get(splitPointIter).setLocation(sl.getLocation());
                ++splitPointIter;
            }
        }
        if (splitPointIter != result.getSplits().size()) {
            throw new ClientException("The location list's length is not correct. Location list size: " + splitPointIter
                + ". Split list size: " + result.getSplits().size());
        }

        return result;
    }

    public static BatchGetRowResponse createBatchGetRowResponse(ResponseContentWithMeta response,
                                                                OtsInternalApi.BatchGetRowResponse
                                                                    batchGetRowResponse) {
        BatchGetRowResponse result = new BatchGetRowResponse(response.getMeta());

        for (OtsInternalApi.TableInBatchGetRowResponse table : batchGetRowResponse.getTablesList()) {
            String tableName = table.getTableName();
            List<OtsInternalApi.RowInBatchGetRowResponse> rowList = table.getRowsList();
            for (int i = 0; i < rowList.size(); i++) {
                result.addResult(OTSProtocolParser.parseBatchGetRowStatus(tableName, rowList.get(i), i));
            }
        }
        return result;
    }

    public static BatchWriteRowResponse createBatchWriteRowResponse(ResponseContentWithMeta response,
                                                                    OtsInternalApi.BatchWriteRowResponse
                                                                        batchWriteRowResponse) {
        BatchWriteRowResponse result = new BatchWriteRowResponse(response.getMeta());

        for (OtsInternalApi.TableInBatchWriteRowResponse table : batchWriteRowResponse.getTablesList()) {
            String tableName = table.getTableName();

            List<OtsInternalApi.RowInBatchWriteRowResponse> statuses = table.getRowsList();
            for (int i = 0; i < statuses.size(); i++) {
                result.addRowResult(OTSProtocolParser.parseBatchWriteRowStatus(tableName, statuses.get(i), i));
            }
        }

        return result;
    }

    public static ListStreamResponse createListStreamResponse(ResponseContentWithMeta response,
                                                              OtsInternalApi.ListStreamResponse listStreamResponse) {
        ListStreamResponse result = new ListStreamResponse(response.getMeta());
        List<Stream> streams = new ArrayList<Stream>();
        for (OtsInternalApi.Stream stream : listStreamResponse.getStreamsList()) {
            streams.add(OTSProtocolParser.parseStream(stream));
        }
        result.setStreams(streams);
        return result;
    }

    public static DescribeStreamResponse createDescribeStreamResponse(ResponseContentWithMeta response,
                                                                      OtsInternalApi.DescribeStreamResponse
                                                                          describeStreamResponse) {
        DescribeStreamResponse result = new DescribeStreamResponse(response.getMeta());
        result.setStreamId(describeStreamResponse.getStreamId());
        result.setExpirationTime(describeStreamResponse.getExpirationTime());
        result.setTableName(describeStreamResponse.getTableName());
        result.setCreationTime(describeStreamResponse.getCreationTime());
        result.setStatus(OTSProtocolParser.parseStreamStatus(describeStreamResponse.getStreamStatus()));
        List<StreamShard> shards = new ArrayList<StreamShard>();
        for (OtsInternalApi.StreamShard shard : describeStreamResponse.getShardsList()) {
            shards.add(OTSProtocolParser.parseStreamShard(shard));
        }
        result.setShards(shards);
        if (describeStreamResponse.hasNextShardId()) {
            result.setNextShardId(describeStreamResponse.getNextShardId());
        }
        return result;
    }

    public static GetShardIteratorResponse createGetShardIteratorResponse(ResponseContentWithMeta response,
                                                                          OtsInternalApi.GetShardIteratorResponse
                                                                              getShardIteratorResponse) {
        GetShardIteratorResponse result = new GetShardIteratorResponse(response.getMeta());
        result.setShardIterator(getShardIteratorResponse.getShardIterator());
        return result;
    }

    public static GetStreamRecordResponse createGetStreamRecordResponse(ResponseContentWithMeta response,
                                                                        OtsInternalApi.GetStreamRecordResponse
                                                                            getStreamRecordResponse) {
        GetStreamRecordResponse result = new GetStreamRecordResponse(response.getMeta());
        if (getStreamRecordResponse.hasNextShardIterator()) {
            result.setNextShardIterator(getStreamRecordResponse.getNextShardIterator());
        }

        List<StreamRecord> records = new ArrayList<StreamRecord>();
        for (OtsInternalApi.GetStreamRecordResponse.StreamRecord respRecord : getStreamRecordResponse
            .getStreamRecordsList()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(respRecord.getRecord().asReadOnlyByteBuffer()));
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row. Row count: " + rows.size());
                }
                PlainBufferRow row = rows.get(0);
                records.add(PlainBufferConversion.toStreamRecord(row, respRecord.getActionType()));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }

        result.setRecords(records);
        return result;
    }

    public static StartLocalTransactionResponse createStartLocalTransactionResponse(ResponseContentWithMeta response,
                                                                                    OtsInternalApi
                                                                                        .StartLocalTransactionResponse startLocalTransactionResponse) {
        StartLocalTransactionResponse result = new StartLocalTransactionResponse(response.getMeta());
        result.setTransactionID(startLocalTransactionResponse.getTransactionId());
        return result;
    }

    public static CommitTransactionResponse createCommitTransactionResponse(ResponseContentWithMeta response,
                                                                            OtsInternalApi.CommitTransactionResponse
                                                                                commitTransactionResponse) {
        return new CommitTransactionResponse(response.getMeta());
    }

    public static AbortTransactionResponse createAbortTransactionResponse(ResponseContentWithMeta response,
                                                                          OtsInternalApi.AbortTransactionResponse
                                                                              abortTransactionResponse) {
        return new AbortTransactionResponse(response.getMeta());
    }

    public static CreateSearchIndexResponse createCreateSearchIndexResponse(ResponseContentWithMeta response,
                                                                            Search.CreateSearchIndexResponse
                                                                                createSearchIndexResponse) {
        return new CreateSearchIndexResponse(response.getMeta());
    }

    public static DeleteSearchIndexResponse createDeleteSearchIndexResponse(ResponseContentWithMeta response,
                                                                            Search.DeleteSearchIndexResponse
                                                                                deleteSearchIndexResponse) {
        return new DeleteSearchIndexResponse(response.getMeta());
    }

    public static ListSearchIndexResponse createListSearchIndexResponse(ResponseContentWithMeta response,
                                                                        Search.ListSearchIndexResponse
                                                                            listSearchIndexResponse) {
        ListSearchIndexResponse result = new ListSearchIndexResponse(response.getMeta());
        List<SearchIndexInfo> indexInfos = new ArrayList<SearchIndexInfo>();
        for (Search.IndexInfo indexInfo : listSearchIndexResponse.getIndicesList()) {
            SearchIndexInfo info = new SearchIndexInfo();
            info.setTableName(indexInfo.getTableName());
            info.setIndexName(indexInfo.getIndexName());
            indexInfos.add(info);
        }
        result.setIndexInfos(indexInfos);
        return result;
    }

    public static DescribeSearchIndexResponse createDescribeSearchIndexResponse(ResponseContentWithMeta response,
                                                                                Search.DescribeSearchIndexResponse
                                                                                    describeSearchIndexResponse) {
        DescribeSearchIndexResponse result = new DescribeSearchIndexResponse(response.getMeta());
        result.setSchema(SearchProtocolParser.toIndexSchema(
            describeSearchIndexResponse.getSchema()));
        if (describeSearchIndexResponse.hasSyncStat()) {
            result.setSyncStat(SearchProtocolParser.toSyncStat(
                describeSearchIndexResponse.getSyncStat()));
        }
        if (describeSearchIndexResponse.hasMeteringInfo()) {
            result.setMeteringInfo(SearchProtocolParser.toMeteringInfo(
                    describeSearchIndexResponse.getMeteringInfo()));
        }
        return result;
    }

    public static SearchResponse createSearchResponse(ResponseContentWithMeta response,
                                                      Search.SearchResponse searchResponse) throws IOException {
        SearchResponse result = new SearchResponse(response.getMeta());
        result.setTotalCount(searchResponse.getTotalHits());
        result.setAllSuccess(searchResponse.getIsAllSucceeded());
        List<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < searchResponse.getRowsCount(); ++i) {
            com.google.protobuf.ByteString bytes = searchResponse.getRows(i);
            PlainBufferCodedInputStream coded = new PlainBufferCodedInputStream(
                new PlainBufferInputStream(bytes.asReadOnlyByteBuffer()));
            List<PlainBufferRow> plainBufferRows = coded.readRowsWithHeader();
            if (plainBufferRows.size() != 1) {
                throw new IOException("Expect only returns one row. Row count: " + rows.size());
            }
            Row row = PlainBufferConversion.toRow(plainBufferRows.get(0));
            rows.add(row);
        }
        if (searchResponse.hasNextToken()) {
            result.setNextToken(searchResponse.getNextToken().toByteArray());
        }
        result.setRows(rows);
        return result;
    }

    public static CreateTunnelResponse createCreateTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.CreateTunnelResponse tunnelResponse) {
        CreateTunnelResponse resp = new CreateTunnelResponse(response.getMeta());
        resp.setTunnelId(tunnelResponse.getTunnelId());
        return resp;
    }

    private static TunnelType createTunnelType(String type) {
        return TunnelType.valueOf(type);
    }

    private static TunnelStage createTunnelStage(String stage) {
        return TunnelStage.valueOf(stage);
    }

    private static TunnelInfo createTunnelInfo(TunnelServiceApi.TunnelInfo tunnelInfo) {
        TunnelInfo actualInfo = new TunnelInfo();
        actualInfo.setTunnelId(tunnelInfo.getTunnelId());
        actualInfo.setTunnelType(createTunnelType(tunnelInfo.getTunnelType()));
        actualInfo.setTableName(tunnelInfo.getTableName());
        actualInfo.setInstanceName(tunnelInfo.getInstanceName());
        actualInfo.setStage(createTunnelStage(tunnelInfo.getStage()));
        actualInfo.setExpired(tunnelInfo.getExpired());
        actualInfo.setTunnelName(tunnelInfo.getTunnelName());
        return actualInfo;
    }

    private static ChannelType createChannelType(String type) {
        return ChannelType.valueOf(type);
    }

    private static ChannelStatus createChannelStatus(String stage) {
        return ChannelStatus.valueOf(stage);
    }

    private static ChannelInfo createChannelInfo(TunnelServiceApi.ChannelInfo channelInfo) {
        ChannelInfo actualInfo = new ChannelInfo();
        actualInfo.setChannelId(channelInfo.getChannelId());
        actualInfo.setChannelType(createChannelType(channelInfo.getChannelType()));
        actualInfo.setChannelStatus(createChannelStatus(channelInfo.getChannelStatus()));
        actualInfo.setClientId(channelInfo.getClientId());
        long rpoMillis = channelInfo.getChannelRpo() / 1000000;
        actualInfo.setChannelConsumePoint(new Date(rpoMillis));
        actualInfo.setChannelRpo(System.currentTimeMillis() - rpoMillis);
        actualInfo.setChannelCount(channelInfo.getChannelCount());
        return actualInfo;
    }

    public static ListTunnelResponse createListTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.ListTunnelResponse tunnelResponse) {
        ListTunnelResponse resp = new ListTunnelResponse(response.getMeta());

        List<TunnelInfo> actualInfos = new ArrayList<TunnelInfo>();
        for (TunnelServiceApi.TunnelInfo tunnelInfo : tunnelResponse.getTunnelsList()) {
            actualInfos.add(createTunnelInfo(tunnelInfo));
        }
        resp.setTunnelInfos(actualInfos);
        return resp;
    }

    public static DescribeTunnelResponse createDescribeTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.DescribeTunnelResponse tunnelResponse) {
        DescribeTunnelResponse resp = new DescribeTunnelResponse(response.getMeta());
        long rpoMillis = tunnelResponse.getTunnelRpo() / 1000000;
        resp.setTunnelConsumePoint(new Date(rpoMillis));
        resp.setTunnelInfo(createTunnelInfo(tunnelResponse.getTunnel()));
        List<ChannelInfo> actualInfos = new ArrayList<ChannelInfo>();
        long tunnelRpo = 0;
        for (TunnelServiceApi.ChannelInfo channelInfo : tunnelResponse.getChannelsList()) {
            ChannelInfo actualChannel = createChannelInfo(channelInfo);
            actualInfos.add(createChannelInfo(channelInfo));
            if (actualChannel.getChannelRpo() > tunnelRpo) {
                tunnelRpo = actualChannel.getChannelRpo();
            }
        }
        resp.setTunnelRpo(tunnelRpo != 0 ? tunnelRpo : System.currentTimeMillis());
        resp.setChannelInfos(actualInfos);
        return resp;
    }

    public static DeleteTunnelResponse createDeleteTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.DeleteTunnelResponse tunnelResponse) {
        return new DeleteTunnelResponse(response.getMeta());
    }

    public static ConnectTunnelResponse createConnectTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.ConnectResponse tunnelResponse) {
        ConnectTunnelResponse resp = new ConnectTunnelResponse(response.getMeta());
        resp.setClientId(tunnelResponse.getClientId());
        return resp;
    }

    public static ChannelStatus createChannelStatus(TunnelServiceApi.ChannelStatus status) {
        switch (status) {
            case OPEN:
                return ChannelStatus.OPEN;
            case CLOSING:
                return ChannelStatus.CLOSING;
            case CLOSE:
                return ChannelStatus.CLOSE;
            case TERMINATED:
                return ChannelStatus.TERMINATED;
            default:
                throw new IllegalArgumentException("Unknown channel type: " + status.name());
        }
    }

    public static Channel createChannel(TunnelServiceApi.Channel channel) {
        Channel actualChannel = new Channel();
        actualChannel.setChannelId(channel.getChannelId());
        actualChannel.setVersion(channel.getVersion());
        actualChannel.setStatus(createChannelStatus(channel.getStatus()));
        return actualChannel;
    }

    public static HeartbeatResponse createHeartbeatResponse(
        ResponseContentWithMeta response, TunnelServiceApi.HeartbeatResponse tunnelResponse) {
        HeartbeatResponse resp = new HeartbeatResponse(response.getMeta());
        List<Channel> channels = new ArrayList<Channel>();
        for (TunnelServiceApi.Channel channel : tunnelResponse.getChannelsList()) {
            channels.add(createChannel(channel));
        }
        resp.setChannels(channels);
        return resp;
    }

    public static ShutdownTunnelResponse createShutdownTunnelResponse(
        ResponseContentWithMeta response, TunnelServiceApi.ShutdownResponse tunnelResponse) {
        return new ShutdownTunnelResponse(response.getMeta());
    }

    public static GetCheckpointResponse createGetCheckpointResponse(
        ResponseContentWithMeta response, TunnelServiceApi.GetCheckpointResponse tunnelResponse) {
        GetCheckpointResponse resp = new GetCheckpointResponse(response.getMeta());
        resp.setCheckpoint(tunnelResponse.getCheckpoint());
        resp.setSequenceNumber(tunnelResponse.getSequenceNumber());
        return resp;
    }

    public static final String FINISH_TAG = "finished";
    public static ReadRecordsResponse createReadRecordsResponse(
        ResponseContentWithMeta response, TunnelServiceApi.ReadRecordsResponse tunnelResponse) {
        ReadRecordsResponse resp = new ReadRecordsResponse(response.getMeta());
        resp.setMemoizedSerializedSize(tunnelResponse.getSerializedSize());
        if (tunnelResponse.hasNextToken()) {
            if (FINISH_TAG.equals(tunnelResponse.getNextToken())) {
                resp.setNextToken(null);
            } else {
                resp.setNextToken(tunnelResponse.getNextToken());
            }
        }

        List<StreamRecord> records = new ArrayList<StreamRecord>();

        for (TunnelServiceApi.Record record : tunnelResponse.getRecordsList()) {
            try {
                PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(
                    new PlainBufferInputStream(record.getRecord().asReadOnlyByteBuffer())
                );
                List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                if (rows.size() != 1) {
                    throw new IOException("Expect only returns one row, Row count: " + rows.size());
                }
                PlainBufferRow row = rows.get(0);
                records.add(PlainBufferConversion.toStreamRecord(row, record.getActionType()));
            } catch (Exception e) {
                throw new ClientException("Failed to parse row", e);
            }
        }

        resp.setRecords(records);
        return resp;
    }

    public static CheckpointResponse createCheckpointResponse(
        ResponseContentWithMeta response, TunnelServiceApi.CheckpointResponse tunnelResponse) {
        return new CheckpointResponse(response.getMeta());
    }
}
