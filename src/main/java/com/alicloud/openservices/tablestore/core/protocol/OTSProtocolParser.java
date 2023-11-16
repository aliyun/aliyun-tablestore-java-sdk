package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.sql.SQLPayloadVersion;
import com.alicloud.openservices.tablestore.model.sql.SQLStatementType;

import java.io.IOException;
import java.util.List;

public class OTSProtocolParser {
    public static PrimaryKeyType toPrimaryKeyType(OtsInternalApi.PrimaryKeyType type) {
        switch (type) {
            case INTEGER:
                return PrimaryKeyType.INTEGER;
            case STRING:
                return PrimaryKeyType.STRING;
            case BINARY:
                return PrimaryKeyType.BINARY;
            case DATETIME:
                return PrimaryKeyType.DATETIME;
            default:
                throw new IllegalArgumentException("Unknown primary key type: " + type);
        }
    }

    public static DefinedColumnType toDefinedColumnType(OtsInternalApi.DefinedColumnType type) {
        switch (type) {
            case DCT_INTEGER:
                return DefinedColumnType.INTEGER;
            case DCT_DOUBLE:
                return DefinedColumnType.DOUBLE;
            case DCT_BOOLEAN:
                return DefinedColumnType.BOOLEAN;
            case DCT_STRING:
                return DefinedColumnType.STRING;
            case DCT_BLOB:
                return DefinedColumnType.BINARY;
            case DCT_DATETIME:
                return DefinedColumnType.DATETIME;
            default:
                throw new IllegalArgumentException("Unknown defined column type: " + type);
        }
    }

    public static PrimaryKeyOption toPrimaryKeyOption(OtsInternalApi.PrimaryKeyOption option) {
        switch (option) {
            case AUTO_INCREMENT:
                return PrimaryKeyOption.AUTO_INCREMENT;
            default:
                throw new IllegalArgumentException("Unknown primary key option: " + option);
        }
    }

    public static TableMeta parseTableMeta(OtsInternalApi.TableMeta tableMeta) {
        TableMeta result = new TableMeta(tableMeta.getTableName());
        for (OtsInternalApi.PrimaryKeySchema pk : tableMeta.getPrimaryKeyList()) {
            if (pk.hasOption()) {
                result.addPrimaryKeyColumn(pk.getName(), toPrimaryKeyType(pk.getType()), toPrimaryKeyOption(pk.getOption()));
            } else {
                result.addPrimaryKeyColumn(pk.getName(), toPrimaryKeyType(pk.getType()));
            }
        }
        for (OtsInternalApi.DefinedColumnSchema defCol : tableMeta.getDefinedColumnList()) {
            DefinedColumnSchema defSchema = new DefinedColumnSchema(defCol.getName(), toDefinedColumnType(defCol.getType()));
            result.addDefinedColumn(defSchema);
        }
        return result;
    }

    public static IndexType parseIndexType(OtsInternalApi.IndexType indexType) {
        switch (indexType) {
            case IT_GLOBAL_INDEX:
                return IndexType.IT_GLOBAL_INDEX;
            case IT_LOCAL_INDEX:
                return IndexType.IT_LOCAL_INDEX;
            default:
                throw new IllegalArgumentException("Unknown index type: " + indexType);
        }
    }

    public static IndexUpdateMode parseIndexUpdateMode(OtsInternalApi.IndexUpdateMode indexUpdateMode) {
        switch (indexUpdateMode) {
            case IUM_ASYNC_INDEX:
                return IndexUpdateMode.IUM_ASYNC_INDEX;
            case IUM_SYNC_INDEX:
                return IndexUpdateMode.IUM_SYNC_INDEX;
            default:
                throw new IllegalArgumentException("Unknown index update mode: " + indexUpdateMode);
        }
    }

    public static IndexMeta parseIndexMeta(OtsInternalApi.IndexMeta indexMeta) {
        IndexMeta result = new IndexMeta(indexMeta.getName());
        for (String pk : indexMeta.getPrimaryKeyList()) {
            result.addPrimaryKeyColumn(pk);
        }
        for (String definedCol : indexMeta.getDefinedColumnList()) {
            result.addDefinedColumn(definedCol);
        }
        result.setIndexType(parseIndexType(indexMeta.getIndexType()));
        result.setIndexUpdateMode(parseIndexUpdateMode(indexMeta.getIndexUpdateMode()));

        return result;
    }

    public static ReservedThroughputDetails parseReservedThroughputDetails(
            OtsInternalApi.ReservedThroughputDetails reservedThroughputDetails) {
        return new ReservedThroughputDetails(
                parseCapacityUnit(reservedThroughputDetails.getCapacityUnit()),
                reservedThroughputDetails.getLastIncreaseTime(),
                reservedThroughputDetails.getLastDecreaseTime());
    }

    public static CapacityUnit parseCapacityUnit(OtsInternalApi.CapacityUnit capacityUnit) {
        CapacityUnit result = new CapacityUnit();
        if (capacityUnit.hasRead()) {
            result.setReadCapacityUnit(capacityUnit.getRead());
        }

        if (capacityUnit.hasWrite()) {
            result.setWriteCapacityUnit(capacityUnit.getWrite());
        }
        return result;
    }

    public static CapacityDataSize parseCapacityDataSize(OtsInternalApi.CapacityDataSize capacityDataSize) {
        CapacityDataSize result = new CapacityDataSize();
        if (capacityDataSize.hasReadSize()) {
            result.setReadCapacityDataSize(capacityDataSize.getReadSize());
        }

        if (capacityDataSize.hasWriteSize()) {
            result.setWriteCapacityDataSize(capacityDataSize.getWriteSize());
        }
        return result;
    }

    public static SQLPayloadVersion parseSQLPayloadVersion(OtsInternalApi.SQLPayloadVersion sqlPayloadVersion) {
        switch (sqlPayloadVersion) {
            case SQL_FLAT_BUFFERS:
                return SQLPayloadVersion.SQL_FLAT_BUFFERS;
            default:
                throw new UnsupportedOperationException("not supported sql payload version: " + sqlPayloadVersion);
        }
    }

    public static SQLStatementType parseSQLStatementType(OtsInternalApi.SQLStatementType sqlStatementType) {
        switch (sqlStatementType) {
            case SQL_SELECT:
                return SQLStatementType.SQL_SELECT;
            case SQL_CREATE_TABLE:
                return SQLStatementType.SQL_CREATE_TABLE;
            case SQL_SHOW_TABLE:
                return SQLStatementType.SQL_SHOW_TABLE;
            case SQL_DESCRIBE_TABLE:
                return SQLStatementType.SQL_DESCRIBE_TABLE;
            case SQL_DROP_TABLE:
                return SQLStatementType.SQL_DROP_TABLE;
            case SQL_ALTER_TABLE:
                return SQLStatementType.SQL_ALTER_TABLE;
            default:
                throw new UnsupportedOperationException("not supported sql type: " + sqlStatementType);
        }
    }

    public static TableOptions parseTableOptions(OtsInternalApi.TableOptions tableOptions) {
        TableOptions result = new TableOptions();

        if (tableOptions.hasDeviationCellVersionInSec()) {
            result.setMaxTimeDeviation(tableOptions.getDeviationCellVersionInSec());
        }

        if (tableOptions.hasMaxVersions()) {
            result.setMaxVersions(tableOptions.getMaxVersions());
        }

        if (tableOptions.hasTimeToLive()) {
            result.setTimeToLive(tableOptions.getTimeToLive());
        }

        if (tableOptions.hasAllowUpdate()) {
            result.setAllowUpdate(tableOptions.getAllowUpdate());
        }

        return result;
    }

    public static BatchGetRowResponse.RowResult parseBatchGetRowStatus(String tableName, OtsInternalApi.RowInBatchGetRowResponse status, int index) {
        if (status.getIsOk()) {
            Row row = null;
            if (!status.getRow().isEmpty()) {
                try {
                    PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(new PlainBufferInputStream(status.getRow().asReadOnlyByteBuffer()));
                    List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                    if (rows.size() != 1) {
                        throw new IOException("Expect only returns one row. Row count: " + rows.size());
                    }
                    row = PlainBufferConversion.toRow(rows.get(0));
                } catch (Exception e) {
                    throw new ClientException("Failed to parse row data.", e);
                }
            }
            ConsumedCapacity consumedCapacity = new ConsumedCapacity(parseCapacityUnit(status.getConsumed().getCapacityUnit()));
            if (status.hasNextToken()) {
                return new BatchGetRowResponse.RowResult(tableName, row, consumedCapacity, index, status.getNextToken().toByteArray());
            } else {
                return new BatchGetRowResponse.RowResult(tableName, row, consumedCapacity, index);
            }
        } else {
            com.alicloud.openservices.tablestore.model.Error error = new com.alicloud.openservices.tablestore.model.Error(status.getError().getCode(), status.getError().getMessage());
            return new BatchGetRowResponse.RowResult(tableName, error, index);
        }
    }

    public static BatchWriteRowResponse.RowResult parseBatchWriteRowStatus(String tableName, OtsInternalApi.RowInBatchWriteRowResponse status, int index) {
	Row row = null;
        if (status.getIsOk()) {
            ConsumedCapacity consumedCapacity = new ConsumedCapacity(parseCapacityUnit(status.getConsumed().getCapacityUnit()));

            if (status.hasRow() && !status.getRow().isEmpty()) {
                try {
                    PlainBufferCodedInputStream inputStream = new PlainBufferCodedInputStream(new PlainBufferInputStream(status.getRow().asReadOnlyByteBuffer()));
                    List<PlainBufferRow> rows = inputStream.readRowsWithHeader();
                    if (rows.size() != 1) {
                        throw new IOException("Expect only returns one row. Row count: " + rows.size());
                    }
                    row = PlainBufferConversion.toRow(rows.get(0));
                } catch (Exception e) {
                    throw new ClientException("Failed to parse row data.", e);
                }
            }
            return new BatchWriteRowResponse.RowResult(tableName, row, consumedCapacity, index);
        } else {
            com.alicloud.openservices.tablestore.model.Error error = new com.alicloud.openservices.tablestore.model.Error(status.getError().getCode(), status.getError().getMessage());
            return new BatchWriteRowResponse.RowResult(tableName, row, error, index);
        }
    }

    public static BulkImportResponse.RowResult parseBulkImportStatus(OtsInternalApi.RowInBulkImportResponse status, int index) {
        if (status.getIsOk()) {
            ConsumedCapacity consumedCapacity = new ConsumedCapacity(
                    OTSProtocolParser.parseCapacityUnit(status.getConsumed().getCapacityUnit()));
            if (status.getConsumed().hasCapacityDataSize()){
                consumedCapacity.setCapacityDataSize(OTSProtocolParser.parseCapacityDataSize(status.getConsumed().getCapacityDataSize()));
            }
            return new BulkImportResponse.RowResult(consumedCapacity, index);
        } else {
            com.alicloud.openservices.tablestore.model.Error error = new com.alicloud.openservices.tablestore.model.Error(status.getError().getCode(), status.getError().getMessage());
            return new BulkImportResponse.RowResult(error, index);
        }
    }


    public static StreamDetails parseStreamDetails(OtsInternalApi.StreamDetails streamDetails) {
        StreamDetails result = new StreamDetails();
        result.setEnableStream(streamDetails.getEnableStream());
        if (streamDetails.hasStreamId()) {
            result.setStreamId(streamDetails.getStreamId());
        }
        if (streamDetails.hasExpirationTime()) {
            result.setExpirationTime(streamDetails.getExpirationTime());
        }
        if (streamDetails.hasLastEnableTime()) {
            result.setLastEnableTime(streamDetails.getLastEnableTime());
        }
        if (!streamDetails.getColumnToGetList().isEmpty()) {
            result.addOriginColumnsToGet(streamDetails.getColumnToGetList());
        }
        return result;
    }

    public static Stream parseStream(OtsInternalApi.Stream stream) {
        Stream result = new Stream();
        result.setStreamId(stream.getStreamId());
        result.setTableName(stream.getTableName());
        result.setCreationTime(stream.getCreationTime());
        return result;
    }

    public static StreamShard parseStreamShard(OtsInternalApi.StreamShard streamShard) {
        StreamShard result = new StreamShard(streamShard.getShardId());
        if (streamShard.hasParentId()) {
            result.setParentId(streamShard.getParentId());
        }
        if (streamShard.hasParentSiblingId()) {
            result.setParentSiblingId(streamShard.getParentSiblingId());
        }
        return result;
    }

    public static StreamStatus parseStreamStatus(OtsInternalApi.StreamStatus status) {
        switch (status) {
            case STREAM_ENABLING:
                return StreamStatus.ENABLING;
            case STREAM_ACTIVE:
                return StreamStatus.ACTIVE;
            default:
                throw new ClientException("Unknown stream status:" + status);
        }
    }

    public static SSEDetails parseSseDetails(OtsInternalApi.SSEDetails sseDetails) {
        SSEDetails result = new SSEDetails();
        result.setEnable(sseDetails.getEnable());
        if (sseDetails.hasKeyType()) {
            OtsInternalApi.SSEKeyType keyType = sseDetails.getKeyType();
            switch (keyType) {
                case SSE_KMS_SERVICE:
                    result.setKeyType(SSEKeyType.SSE_KMS_SERVICE);
                    break;
                case SSE_BYOK:
                    result.setKeyType(SSEKeyType.SSE_BYOK);
                    break;
                default:
                    throw new ClientException("Unknown server side encryption key type: " + keyType);
            }
        }
        if (sseDetails.hasKeyId()) {
            result.setKeyId(sseDetails.getKeyId().toByteArray());
        }
        if (sseDetails.hasRoleArn()) {
            result.setRoleArn(sseDetails.getRoleArn().toByteArray());
        }
        return result;
    }
}
