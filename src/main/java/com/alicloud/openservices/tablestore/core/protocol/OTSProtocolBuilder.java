package com.alicloud.openservices.tablestore.core.protocol;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.ColumnConditionType;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.filter.*;
import com.alicloud.openservices.tablestore.model.sql.SQLPayloadVersion;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;
import com.google.protobuf.ByteString;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.google.protobuf.Message;

public class OTSProtocolBuilder {

    public static OtsInternalApi.PrimaryKeyType toPBPrimaryKeyType(PrimaryKeyType type) {
        switch(type) {
        case INTEGER:
            return OtsInternalApi.PrimaryKeyType.INTEGER;
        case STRING:
            return OtsInternalApi.PrimaryKeyType.STRING;
        case BINARY:
            return OtsInternalApi.PrimaryKeyType.BINARY;
        case DATETIME:
            return OtsInternalApi.PrimaryKeyType.DATETIME;

        default:
            throw new IllegalArgumentException("Unknown primary key type: " + type);
        }
    }

    public static OtsInternalApi.PrimaryKeyOption toPBPrimaryKeyOption(PrimaryKeyOption option) {
        switch(option) {
            case AUTO_INCREMENT:
                return OtsInternalApi.PrimaryKeyOption.AUTO_INCREMENT;
            default:
                throw new IllegalArgumentException("Unknown primary key option: " + option);
        }
    }

    public static OtsInternalApi.DefinedColumnType toPBDefinedColumnType(DefinedColumnType type) {
        switch (type) {
            case INTEGER:
                return OtsInternalApi.DefinedColumnType.DCT_INTEGER;
            case DOUBLE:
                return OtsInternalApi.DefinedColumnType.DCT_DOUBLE;
            case BOOLEAN:
                return OtsInternalApi.DefinedColumnType.DCT_BOOLEAN;
            case STRING:
                return OtsInternalApi.DefinedColumnType.DCT_STRING;
            case BINARY:
                return OtsInternalApi.DefinedColumnType.DCT_BLOB;
            case DATETIME:
                return OtsInternalApi.DefinedColumnType.DCT_DATETIME;
            default:
                throw new IllegalArgumentException("Unknown defined column type: " + type);
        }
    }

    public static OtsInternalApi.IndexType toPBIndexType(IndexType indexType) {
        switch (indexType) {
            case IT_GLOBAL_INDEX:
                return OtsInternalApi.IndexType.IT_GLOBAL_INDEX;
            case IT_LOCAL_INDEX:
                return OtsInternalApi.IndexType.IT_LOCAL_INDEX;
            default:
                throw new IllegalArgumentException("Unknown index type:" + indexType);
        }
    }

    public static OtsInternalApi.IndexUpdateMode toPBIndexUpdateMode(IndexUpdateMode indexUpdateMode) {
        switch (indexUpdateMode) {
            case IUM_ASYNC_INDEX:
                return OtsInternalApi.IndexUpdateMode.IUM_ASYNC_INDEX;
            case IUM_SYNC_INDEX:
                return OtsInternalApi.IndexUpdateMode.IUM_SYNC_INDEX;
            default:
                throw new IllegalArgumentException("Unknown index update mode" + indexUpdateMode);
        }
    }

    public static OtsDelivery.EventColumn.eventTimeFormat toPBEventTimeFormat(EventTimeFormat eventTimeFormat) {
        switch(eventTimeFormat) {
            case RFC822:
                return OtsDelivery.EventColumn.eventTimeFormat.RFC822;
            case RFC850:
                return OtsDelivery.EventColumn.eventTimeFormat.RFC850;
            case RFC1123:
                return OtsDelivery.EventColumn.eventTimeFormat.RFC1123;
            case RFC3339:
                return OtsDelivery.EventColumn.eventTimeFormat.RFC3339;
            case Unix:
                return OtsDelivery.EventColumn.eventTimeFormat.Unix;
            default:
                throw new IllegalArgumentException("Unknown eventTimeFormat: " + eventTimeFormat);
        }
    }

    public static OtsDelivery.TimeFormatter toPBTimeFormatter(TimeFormatter timeFormatter) {
        switch (timeFormatter) {
            case YDMFormatter:
                return OtsDelivery.TimeFormatter.YDMFormatter;
            default:
                throw new IllegalArgumentException("Unknown timeFormatter: " + timeFormatter);
        }
    }

    public static OtsDelivery.Format toPBFormat(OSSFileFormat format) {
        switch (format) {
            case Parquet:
                return OtsDelivery.Format.Parquet;
            default:
                throw new IllegalArgumentException("UnKnown format: " + format);
        }
    }

    public static OtsDelivery.DeliveryTaskType toPBDeliveryTaskType(DeliveryTaskType deliveryTaskType) {
        switch (deliveryTaskType) {
            case BASE:
                return OtsDelivery.DeliveryTaskType.BASE;
            case INC:
                return OtsDelivery.DeliveryTaskType.INC;
            case BASE_INC:
                return OtsDelivery.DeliveryTaskType.BASE_INC;
            default:
                throw new IllegalArgumentException("UnKnown deliveryTaskType: " + deliveryTaskType);
        }
    }

    public static OtsDelivery.ParquetSchema.DataType toPBDataType(DataType dataType) {
        switch (dataType) {
            case BOOL:
                return OtsDelivery.ParquetSchema.DataType.BOOL;
            case INT64:
                return OtsDelivery.ParquetSchema.DataType.INT64;
            case UTF8:
                return OtsDelivery.ParquetSchema.DataType.UTF8;
            case DOUBLE:
                return OtsDelivery.ParquetSchema.DataType.DOUBLE;
            case DATE:
                return OtsDelivery.ParquetSchema.DataType.DATE;
            case DECIMAL:
                return OtsDelivery.ParquetSchema.DataType.DECIMAL;
            case TIME_MILLIS:
                return OtsDelivery.ParquetSchema.DataType.TIME_MILLIS;
            case TIME_MICROS:
                return OtsDelivery.ParquetSchema.DataType.TIME_MICROS;
            case LIST:
                return OtsDelivery.ParquetSchema.DataType.LIST;
            case MAP:
                return OtsDelivery.ParquetSchema.DataType.MAP;
            case INTERVAL:
                return OtsDelivery.ParquetSchema.DataType.INTERVAL;
            case FLOAT:
                return OtsDelivery.ParquetSchema.DataType.FLOAT;
            case INT32:
                return OtsDelivery.ParquetSchema.DataType.INT32;
            case INT96:
                return OtsDelivery.ParquetSchema.DataType.INT96;
            default:
                throw new IllegalArgumentException("UnKnown dataType: " + dataType);
        }
    }

    public static OtsDelivery.Encoding toPBEncoding(OSSFileEncoding encoding) {
        switch (encoding) {
            case PLAIN:
                return OtsDelivery.Encoding.PLAIN;
            case PLAIN_DICTIONARY:
                return OtsDelivery.Encoding.PLAIN_DICTIONARY;
            case DELTA_BINARY_PACKED:
                return OtsDelivery.Encoding.DELTA_BINARY_PACKED;
            case DELTA_BYTE_ARRAY:
                return OtsDelivery.Encoding.DELTA_BYTE_ARRAY;
            case DELTA_LENGTH_BYTE_ARRAY:
                return OtsDelivery.Encoding.DELTA_LENGTH_BYTE_ARRAY;
            default:
                throw new IllegalArgumentException("UnKnown encoding: " + encoding);
        }
    }

    public static OtsInternalApi.DefinedColumnSchema buildDefinedColumnSchema(DefinedColumnSchema definedColun) {
        OtsInternalApi.DefinedColumnSchema.Builder builder = OtsInternalApi.DefinedColumnSchema.newBuilder();
        builder.setName(definedColun.getName());
        builder.setType(toPBDefinedColumnType(definedColun.getType()));
        return builder.build();
    }

    public static OtsInternalApi.IndexMeta buildIndexMeta(IndexMeta indexMeta) {
        OtsInternalApi.IndexMeta.Builder builder = OtsInternalApi.IndexMeta.newBuilder();
        builder.setName(indexMeta.getIndexName());
        for (String pk : indexMeta.getPrimaryKeyList()) {
            builder.addPrimaryKey(pk);
        }
        for (String definedCol : indexMeta.getDefinedColumnsList()) {
            builder.addDefinedColumn(definedCol);
        }
        builder.setIndexType(toPBIndexType(indexMeta.getIndexType()));
        builder.setIndexUpdateMode(toPBIndexUpdateMode(indexMeta.getIndexUpdateMode()));
        return builder.build();
    }

    public static OtsInternalApi.TableMeta buildTableMeta(TableMeta tableMeta) {
        OtsInternalApi.TableMeta.Builder builder = OtsInternalApi.TableMeta.newBuilder();
        builder.setTableName(tableMeta.getTableName());
        for (PrimaryKeySchema pk : tableMeta.getPrimaryKeyList()) {
            builder.addPrimaryKey(buildPrimaryKeySchema(pk));
        }
        for (DefinedColumnSchema defCol : tableMeta.getDefinedColumnsList()) {
            builder.addDefinedColumn(buildDefinedColumnSchema(defCol));
        }

        return builder.build();
    }

    private static OtsInternalApi.PrimaryKeySchema buildPrimaryKeySchema(PrimaryKeySchema pk) {
        OtsInternalApi.PrimaryKeySchema.Builder builder = OtsInternalApi.PrimaryKeySchema.newBuilder();
        builder.setName(pk.getName());
        builder.setType(toPBPrimaryKeyType(pk.getType()));
        if (pk.hasOption()) {
            builder.setOption(toPBPrimaryKeyOption(pk.getOption()));
        }
        return builder.build();
    }

    public static OtsInternalApi.GetRowRequest buildGetRowRequest(GetRowRequest request) {
        SingleRowQueryCriteria criteria = request.getRowQueryCriteria();

        OtsInternalApi.GetRowRequest.Builder builder = OtsInternalApi.GetRowRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(criteria.getTableName());

        // required bytes primary_key = 2;
        try {
            builder.setPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.getPrimaryKey())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize primary key failed.", e);
        }

        // repeated string columns_to_get = 3;
        for (String column : criteria.getColumnsToGet()) {
            builder.addColumnsToGet(column);
        }

        // optional TimeRange time_range = 4;
        boolean onlyOneVersionInTimeRange = false;
        if (criteria.hasSetTimeRange()) {
            builder.setTimeRange(buildTimeRange(criteria.getTimeRange()));
            onlyOneVersionInTimeRange = criteria.getTimeRange().containsOnlyOneVersion();
        }

        // optional int32 max_versions = 5;
        if (criteria.hasSetMaxVersions() && !onlyOneVersionInTimeRange) {
            builder.setMaxVersions(criteria.getMaxVersions());
        }

        // optional bytes filter = 7;
        if (criteria.hasSetFilter()) {
            builder.setFilter(buildFilter(criteria.getFilter()));
        }

        // optional string start_column = 8;
        if (criteria.hasSetStartColumn()) {
            builder.setStartColumn(criteria.getStartColumn());
        }

        // optional string end_column = 9;
        if (criteria.hasSetEndColumn()) {
            builder.setEndColumn(criteria.getEndColumn());
        }

        // optional bytes token = 10;
        if (criteria.hasSetToken()) {
            builder.setToken(ByteString.copyFrom(criteria.getToken()));
        }

        // optional bytes transaction_id = 11;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }
        return builder.build();
    }

    public static OtsInternalApi.PartitionRange buildPartitionRange(PrimaryKeyValue leftPoint, PrimaryKeyValue rightPoint){
        OtsInternalApi.PartitionRange.Builder builderPartition = OtsInternalApi.PartitionRange.newBuilder();
        try {
            builderPartition.setBegin(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyValueWithoutLengthPrefix(leftPoint)));
        } catch (Exception e) {
            //Bug: serialize primary key value failed.
            throw new ClientException(" Failed to build the start point of partition range.", e);
        }
        try {
            builderPartition.setEnd(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyValueWithoutLengthPrefix(rightPoint)));
        } catch (Exception e) {
            //Bug: serialize primary key value failed.
            throw new ClientException(" Failed to build the end point of partition range.", e);
        }
        return builderPartition.build();
    }

    public static void buildPartitionRanges(OtsInternalApi.CreateTableRequest.Builder builder, List<PrimaryKeyValue> splitPoints){
        if (splitPoints.isEmpty()) {
            return;
        }
        PrimaryKeyValue lastPoint = null;
        for (PrimaryKeyValue currentPoint : splitPoints){
            if (lastPoint == null) {
                builder.addPartitions(buildPartitionRange(PrimaryKeyValue.INF_MIN, currentPoint));
            } else{
                builder.addPartitions(buildPartitionRange(lastPoint, currentPoint));
            }
            lastPoint = currentPoint;
        }
        builder.addPartitions(buildPartitionRange(lastPoint, PrimaryKeyValue.INF_MAX));
    }


    private static OtsInternalApi.TimeRange buildTimeRange(TimeRange timeRange) {
        OtsInternalApi.TimeRange.Builder builder = OtsInternalApi.TimeRange.newBuilder();
        if (timeRange.containsOnlyOneVersion()) {
            builder.setSpecificTime(timeRange.getStart());
        } else {
            builder.setStartTime(timeRange.getStart());
            builder.setEndTime(timeRange.getEnd());
        }
        return builder.build();
    }

    public static OtsInternalApi.CreateTableRequest.Builder getCreateTableRequestBuilder(CreateTableRequest createTableRequest) {
        OtsInternalApi.CreateTableRequest.Builder builder = OtsInternalApi.CreateTableRequest.newBuilder();

        // required TableMeta table_meta = 1;
        builder.setTableMeta(buildTableMeta(createTableRequest.getTableMeta()));

        // required ReservedThroughput reserved_throughput = 2;
        builder.setReservedThroughput(buildReservedThroughput(createTableRequest.getReservedThroughput()));

        // optional TableOptions table_options = 3;
        TableOptions tableOptions = createTableRequest.getTableOptions();
        if (!tableOptions.hasSetMaxVersions() || !tableOptions.hasSetTimeToLive()) {
            throw new IllegalArgumentException("The maxVersions and timeToLive must be set while creating table.");
        }
        builder.setTableOptions(buildTableOptions(tableOptions));

        StreamSpecification streamSpec = createTableRequest.getStreamSpecification();
        if (streamSpec != null) {
            builder.setStreamSpec(buildStreamSpecification(streamSpec));
        }

        SSESpecification sseSpec = createTableRequest.getSseSpecification();
        if (sseSpec != null) {
            builder.setSseSpec(buildSseSpecification(sseSpec));
        }

        List<IndexMeta> indexMeta = createTableRequest.getIndexMetaList();
        for (IndexMeta index : indexMeta) {
            builder.addIndexMetas(buildIndexMeta(index));
        }

        if (createTableRequest.hasLocalTxnSet()) {
            builder.setEnableLocalTxn(createTableRequest.isLocalTxnEnabled());
        }

        return builder;
    }

    public static OtsInternalApi.CreateTableRequest buildCreateTableRequest(CreateTableRequest createTableRequest) {
        return getCreateTableRequestBuilder(createTableRequest).build();
    }

    public static OtsInternalApi.CreateTableRequest buildCreateTableExRequest(CreateTableRequestEx createTableRequestEx) {
        OtsInternalApi.CreateTableRequest.Builder builder = getCreateTableRequestBuilder(createTableRequestEx);

        List<PrimaryKeyValue> splitPoints = createTableRequestEx.getSplitPoints();
        buildPartitionRanges(builder, splitPoints);

        return builder.build();
    }

    private static OtsInternalApi.TableOptions buildTableOptions(TableOptions x) {
        OtsInternalApi.TableOptions.Builder builder = OtsInternalApi.TableOptions.newBuilder();

        if (x.hasSetMaxVersions()) {
            builder.setMaxVersions(x.getMaxVersions());
        }

        if (x.hasSetTimeToLive()) {
            builder.setTimeToLive(x.getTimeToLive());
        }

        if (x.hasSetMaxTimeDeviation()) {
            builder.setDeviationCellVersionInSec(x.getMaxTimeDeviation());
        }

        if (x.hasSetAllowUpdate()) {
            builder.setAllowUpdate(x.getAllowUpdate());
        }

        return builder.build();
    }

    private static OtsInternalApi.ReservedThroughput buildReservedThroughput(ReservedThroughput reservedThroughput) {
        OtsInternalApi.ReservedThroughput.Builder rtBuilder = OtsInternalApi.ReservedThroughput.newBuilder();

        CapacityUnit capacityUnit = reservedThroughput.getCapacityUnit();
        OtsInternalApi.CapacityUnit.Builder builder = OtsInternalApi.CapacityUnit.newBuilder();

        if (capacityUnit.hasSetReadCapacityUnit()) {
            builder.setRead(capacityUnit.getReadCapacityUnit());
        }

        if (capacityUnit.hasSetWriteCapacityUnit()) {
            builder.setWrite(capacityUnit.getWriteCapacityUnit());
        }

        rtBuilder.setCapacityUnit(builder.build());
        return rtBuilder.build();
    }

    public static OtsInternalApi.DeleteTableRequest buildDeleteTableRequest(DeleteTableRequest req) {
        OtsInternalApi.DeleteTableRequest.Builder builder = OtsInternalApi.DeleteTableRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(req.getTableName());
        return builder.build();
    }

    public static OtsDelivery.CreateDeliveryTaskRequest buildCreateDeliveryTaskRequest(CreateDeliveryTaskRequest req) {
        OtsDelivery.CreateDeliveryTaskRequest.Builder builder = OtsDelivery.CreateDeliveryTaskRequest.newBuilder();

        //optional string tableName = 1;
        builder.setTableName(req.getTableName());

        //optional string taskName = 2;
        builder.setTaskName(req.getTaskName());

        //optional OSSTaskConfig taskConfig = 3;
        builder.setTaskConfig(buildOSSTaskConfig(req.getTaskConfig()));

        //optional DeliveryTaskType taskType = 4;
        builder.setTaskType(toPBDeliveryTaskType(req.getTaskType()));
        return builder.build();
    }

    public static OtsDelivery.OSSTaskConfig buildOSSTaskConfig(OSSTaskConfig taskConfig) {
        OtsDelivery.OSSTaskConfig.Builder builder = OtsDelivery.OSSTaskConfig.newBuilder();

        builder.setOssPrefix(taskConfig.getOssPrefix());
        builder.clearFormatter();
//        builder.setFormatter(toPBTimeFormatter(taskConfig.getTimeFormatter()));
        builder.setOssBucket(taskConfig.getOssBucket());
        builder.setOssEndpoint(taskConfig.getOssEndpoint());
        builder.setOssStsRole(taskConfig.getOssStsRole());
        if (taskConfig.getEventTimeColumn() != null) {
            builder.setEventTimeColumn(buildEventTimeColumn(taskConfig.getEventTimeColumn()));
        }
        builder.setFormat(toPBFormat(taskConfig.getFormat()));
        for (ParquetSchema ps : taskConfig.getParquetSchemaList()) {
            builder.addSchema(buildParquetSchema(ps));
        }
        return builder.build();
    }

    private static OtsDelivery.ParquetSchema buildParquetSchema(ParquetSchema parquetSchema) {
        OtsDelivery.ParquetSchema.Builder builder = OtsDelivery.ParquetSchema.newBuilder();
        builder.setColumnName(parquetSchema.getColumnName());
        builder.setOssColumnName(parquetSchema.getOssColumnName());
        builder.setType(toPBDataType(parquetSchema.getType()));
        builder.setEncode(toPBEncoding(parquetSchema.getEncode()));
        if (parquetSchema.getTypeExtend() != null) {
            builder.setTypeExtend(parquetSchema.getTypeExtend());
        }
        return builder.build();
    }

    public static OtsDelivery.EventColumn buildEventTimeColumn(EventColumn eventColumn) {
        OtsDelivery.EventColumn.Builder builder = OtsDelivery.EventColumn.newBuilder();

        builder.setColumnName(eventColumn.getColumnName());
        builder.setTimeFormat(toPBEventTimeFormat(eventColumn.getEventTimeFormat()));
        return builder.build();
    }

    public static OtsDelivery.DeleteDeliveryTaskRequest buildDeleteDeliveryTaskRequest(DeleteDeliveryTaskRequest req) {
        OtsDelivery.DeleteDeliveryTaskRequest.Builder builder = OtsDelivery.DeleteDeliveryTaskRequest.newBuilder();

        //optional string tableName = 1;
        builder.setTableName(req.getTableName());

        //optional string taskName = 2;
        builder.setTaskName(req.getTaskName());
        return builder.build();
    }

    public static OtsDelivery.DescribeDeliveryTaskRequest buildDescribeDeliveryTaskRequest(DescribeDeliveryTaskRequest req) {
        OtsDelivery.DescribeDeliveryTaskRequest.Builder builder = OtsDelivery.DescribeDeliveryTaskRequest.newBuilder();

        //optional string tableName = 1;
        builder.setTableName(req.getTableName());

        //optional string taskName = 2;
        builder.setTaskName(req.getTaskName());
        return builder.build();
    }

    public static OtsDelivery.ListDeliveryTaskRequest buildListDeliveryTaskRequest(ListDeliveryTaskRequest req) {
        OtsDelivery.ListDeliveryTaskRequest.Builder builder = OtsDelivery.ListDeliveryTaskRequest.newBuilder();

        //optional string tableName = 1;
        builder.setTableName(req.getTableName());
        return builder.build();
    }

    public static OtsInternalApi.CreateIndexRequest buildCreateIndexRequest(CreateIndexRequest req) {
        OtsInternalApi.CreateIndexRequest.Builder builder = OtsInternalApi.CreateIndexRequest.newBuilder();

        builder.setMainTableName(req.getMainTableName());
        builder.setIndexMeta(buildIndexMeta(req.getIndexMeta()));
        builder.setIncludeBaseData(req.getIncludeBaseData());
        return builder.build();
    }
    public static OtsInternalApi.DropIndexRequest buildDeleteIndexRequest(DeleteIndexRequest req) {
        OtsInternalApi.DropIndexRequest.Builder builder = OtsInternalApi.DropIndexRequest.newBuilder();

        builder.setMainTableName(req.getMainTableName());
        builder.setIndexName(req.getIndexName());
        return builder.build();
    }

    public static OtsInternalApi.AddDefinedColumnRequest buildAddDefinedColumnRequest(AddDefinedColumnRequest req) {
        OtsInternalApi.AddDefinedColumnRequest.Builder builder = OtsInternalApi.AddDefinedColumnRequest.newBuilder();

        builder.setTableName(req.getTableName());
        List<DefinedColumnSchema> defColumns = req.getDefinedColumn();
        for (DefinedColumnSchema col : defColumns) {
            builder.addColumns(buildDefinedColumnSchema(col));
        }

        return builder.build();
    }

    public static OtsInternalApi.DeleteDefinedColumnRequest buildDeleteDefinedColumnRequest(DeleteDefinedColumnRequest req) {
        OtsInternalApi.DeleteDefinedColumnRequest.Builder builder = OtsInternalApi.DeleteDefinedColumnRequest.newBuilder();

        builder.setTableName(req.getTableName());
        List<String> defColumns = req.getDefinedColumn();
        for (String col : defColumns) {
            builder.addColumns(col);
        }

        return builder.build();
    }

    public static OtsInternalApi.RowExistenceExpectation toPBRowExistenceExpectation(
            RowExistenceExpectation rowExistenceExpectation) {
        switch(rowExistenceExpectation) {
        case EXPECT_EXIST:
            return OtsInternalApi.RowExistenceExpectation.EXPECT_EXIST;
        case EXPECT_NOT_EXIST:
            return OtsInternalApi.RowExistenceExpectation.EXPECT_NOT_EXIST;
        case IGNORE:
            return OtsInternalApi.RowExistenceExpectation.IGNORE;
        default:
            throw new IllegalArgumentException("Invalid row existence expectation: " + rowExistenceExpectation);
        }
    }

    public static OtsInternalApi.Condition buildCondition(Condition cond)
    {
    	OtsInternalApi.Condition.Builder builder = OtsInternalApi.Condition.newBuilder();
        builder.setRowExistence(toPBRowExistenceExpectation(cond.getRowExistenceExpectation()));
        if (cond.getColumnCondition() != null) {
            builder.setColumnCondition(buildFilter(cond.getColumnCondition()));
        }

        return builder.build();
    }

    public static OtsInternalApi.ReturnContent buildReturnContent(ReturnType returnType, Set<String> returnColumnNames)
    {
    	OtsInternalApi.ReturnContent.Builder builder = OtsInternalApi.ReturnContent.newBuilder();
        builder.setReturnType(toPBReturnType(returnType));

        for (String column : returnColumnNames) {
            builder.addReturnColumnNames(column);
        }

        return builder.build();
    }

    private static OtsInternalApi.ReturnType toPBReturnType(ReturnType returnType)
    {
        switch(returnType) {
        case RT_NONE:
            return OtsInternalApi.ReturnType.RT_NONE;
        case RT_PK:
            return OtsInternalApi.ReturnType.RT_PK;
        case RT_AFTER_MODIFY:
            return OtsInternalApi.ReturnType.RT_AFTER_MODIFY;
        default:
            throw new IllegalArgumentException("Invalid return type: " + returnType);
        }
	}

	public static OtsInternalApi.DeleteRowRequest buildDeleteRowRequest(DeleteRowRequest request) {
        OtsInternalApi.DeleteRowRequest.Builder builder = OtsInternalApi.DeleteRowRequest.newBuilder();

        RowDeleteChange rowChange = request.getRowChange();
        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());
        try {
            // required bytes primary_key = 2;
            builder.setPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildRowDeleteChangeWithHeader(rowChange)));
            // required Condition condition = 3;
            builder.setCondition(buildCondition(rowChange.getCondition()));
            // option ReturnType = 4;
            builder.setReturnContent(buildReturnContent(rowChange.getReturnType(), rowChange.getReturnColumnNames()));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize row delete change failed.", e);
        }

        // optional bytes transaction_id = 5;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }
        return builder.build();
    }

    public static OtsInternalApi.PutRowRequest buildPutRowRequest(PutRowRequest request) {
        OtsInternalApi.PutRowRequest.Builder builder = OtsInternalApi.PutRowRequest.newBuilder();
        RowPutChange rowChange = request.getRowChange();
        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());
        try {
            // required bytes row = 2;
            builder.setRow(ByteString.copyFrom(PlainBufferBuilder.buildRowPutChangeWithHeader(rowChange)));
            // required Condition condition = 3;
            builder.setCondition(buildCondition(rowChange.getCondition()));
            // option ReturnType = 4;
            builder.setReturnContent(buildReturnContent(rowChange.getReturnType(), rowChange.getReturnColumnNames()));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize row put change failed.", e);
        }

        // optional bytes transaction_id = 5;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }

        return builder.build();
    }

    public static OtsInternalApi.ListTableRequest buildListTableRequest() {
        OtsInternalApi.ListTableRequest.Builder builder = OtsInternalApi.ListTableRequest.newBuilder();
        return builder.build();
    }

    public static OtsInternalApi.DescribeTableRequest buildDescribeTableRequest(DescribeTableRequest req) {
        OtsInternalApi.DescribeTableRequest.Builder builder = OtsInternalApi.DescribeTableRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(req.getTableName());

        return builder.build();
    }

    public static OtsInternalApi.UpdateRowRequest buildUpdateRowRequest(UpdateRowRequest request) {
        OtsInternalApi.UpdateRowRequest.Builder builder = OtsInternalApi.UpdateRowRequest.newBuilder();
        RowUpdateChange rowChange = request.getRowChange();

        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());
        try {
            // required bytes row_change = 2;
            builder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowUpdateChangeWithHeader(rowChange)));
            // required Condition condition = 3;
            builder.setCondition(buildCondition(rowChange.getCondition()));
            // option ReturnType = 4;
            builder.setReturnContent(buildReturnContent(rowChange.getReturnType(), rowChange.getReturnColumnNames()));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize row update change failed.", e);
        }

        // optional bytes transaction_id = 5;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }

        return builder.build();
    }

    public static OtsInternalApi.Direction toPBDirection(Direction direction) {
        switch (direction) {
        case BACKWARD:
            return OtsInternalApi.Direction.BACKWARD;
        case FORWARD:
            return OtsInternalApi.Direction.FORWARD;
        default:
            throw new IllegalArgumentException("Invalid direction type: " + direction);
        }
    }

    public static OtsInternalApi.ComputeSplitPointsBySizeRequest buildComputeSplitsBySizeRequest(ComputeSplitsBySizeRequest req) {
        OtsInternalApi.ComputeSplitPointsBySizeRequest.Builder builder = OtsInternalApi.ComputeSplitPointsBySizeRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(req.getTableName());
        // required int64 split_size = 2;
        builder.setSplitSize(req.getSplitUnitCount());
        // optional int64 split_size_unit_in_byte = 3;
        builder.setSplitSizeUnitInByte(req.getSplitUnitSizeInByte());
        // optional int64 split_limit = 4;
        if (req.hasSetSplitPointLimit()) {
            builder.setSplitPointLimit(req.getSplitPointLimit());
        }
        return builder.build();
    }

    public static OtsInternalApi.GetRangeRequest buildGetRangeRequest(GetRangeRequest request) {
        RangeRowQueryCriteria criteria = request.getRangeRowQueryCriteria();
        OtsInternalApi.GetRangeRequest.Builder builder = OtsInternalApi.GetRangeRequest.newBuilder();
        // required string table_name = 1;
        builder.setTableName(criteria.getTableName());

        // required Direction direction = 2;
        builder.setDirection(toPBDirection(criteria.getDirection()));

        // repeated string columns_to_get = 3;
        for (String column : criteria.getColumnsToGet()) {
            builder.addColumnsToGet(column);
        }

        // optional TimeRange time_range = 4;
        boolean onlyOneVersionInTimeRange = false;
        if (criteria.hasSetTimeRange()) {
            builder.setTimeRange(buildTimeRange(criteria.getTimeRange()));
            onlyOneVersionInTimeRange = criteria.getTimeRange().containsOnlyOneVersion();
        }

        // optional int32 max_versions = 5;
        if (criteria.hasSetMaxVersions() && !onlyOneVersionInTimeRange) {
            builder.setMaxVersions(criteria.getMaxVersions());
        }

        // optional int32 limit = 6;
        if (criteria.getLimit() > 0) {
            builder.setLimit(criteria.getLimit());
        }

        try {
            // required bytes inclusive_start_primary_key = 7;
            builder.setInclusiveStartPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.getInclusiveStartPrimaryKey())));
            // required bytes exclusive_end_primary_key = 8;
            builder.setExclusiveEndPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.getExclusiveEndPrimaryKey())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize primary key failed.", e);
        }

        // optional bytes filter = 10;
        if (criteria.hasSetFilter()) {
            builder.setFilter(buildFilter(criteria.getFilter()));
        }

        // optional string start_column = 11;
        if (criteria.hasSetStartColumn()) {
            builder.setStartColumn(criteria.getStartColumn());
        }

        // optional string end_column = 12;
        if (criteria.hasSetEndColumn()) {
            builder.setEndColumn(criteria.getEndColumn());
        }

        // optional bytes token = 13;
        if (criteria.hasSetToken()) {
            builder.setToken(ByteString.copyFrom(criteria.getToken()));
        }

        // optional bytes transaction_id = 14;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }

        return builder.build();
    }

    public static OtsInternalApi.BulkExportRequest buildBulkExportRequest(BulkExportRequest request) {
        BulkExportQueryCriteria criteria = request.getBulkExportQueryCriteria();
        OtsInternalApi.BulkExportRequest.Builder builder = OtsInternalApi.BulkExportRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(criteria.getTableName());

        // repeated string columns_to_get = 2;
        for (String column : criteria.getColumnsToGet()) {
            builder.addColumnsToGet(column);
        }

        try {
            // required bytes inclusive_start_primary_key = 3;
            builder.setInclusiveStartPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.getInclusiveStartPrimaryKey())));
            // required bytes exclusive_end_primary_key = 4;
            builder.setExclusiveEndPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.getExclusiveEndPrimaryKey())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize primary key failed.", e);
        }

        // optional bytes filter = 5;
        if (criteria.hasSetFilter()) {
            builder.setFilter(buildFilter(criteria.getFilter()));
        }

        //  optional DataBlockType data_block_type_hint = 6 [default = DBT_SIMPLE_ROW_MATRIX];
        if (criteria.getDataBlockType() == DataBlockType.DBT_PLAIN_BUFFER) {
            builder.setDataBlockTypeHint(OtsInternalApi.DataBlockType.DBT_PLAIN_BUFFER);
        } else {
            builder.setDataBlockTypeHint(OtsInternalApi.DataBlockType.DBT_SIMPLE_ROW_MATRIX);
        }

        return builder.build();
    }

    public static OtsInternalApi.BatchGetRowRequest buildBatchGetRowRequest(
            Map<String, MultiRowQueryCriteria> criteriasGroupByTable) {
        OtsInternalApi.BatchGetRowRequest.Builder builder = OtsInternalApi.BatchGetRowRequest.newBuilder();
        // repeated TableInBatchGetRowResponse tables = 1;
        for (Entry<String, MultiRowQueryCriteria> entry : criteriasGroupByTable.entrySet()) {
            String tableName = entry.getKey();
            MultiRowQueryCriteria criteria = entry.getValue();

            OtsInternalApi.TableInBatchGetRowRequest.Builder tableBuilder = OtsInternalApi.TableInBatchGetRowRequest.newBuilder();
            // required string table_name = 1;
            tableBuilder.setTableName(tableName);

            if (criteria.getRowKeys().size() != criteria.getTokens().size()) {
                throw new ClientException("The number of primaryKeys and tokens must be the same.");
            }

            // repeated bytes primary_key = 2;
            // repeated bytes tokens = 3;
            for (int i = 0; i < criteria.getRowKeys().size(); i++) {
                try {
                    tableBuilder.addPrimaryKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(criteria.get(i))));
                    tableBuilder.addToken(ByteString.copyFrom(criteria.getTokens().get(i)));
                } catch (IOException e) {
                    throw new ClientException("Bug: serialize primary key failed.", e);
                }
            }

            // repeated string columns_to_get = 4;
            for (String column : criteria.getColumnsToGet()) {
                tableBuilder.addColumnsToGet(column);
            }

            // optional TimeRange time_range = 5;
            boolean onlyOneVersionInTimeRange = false;
            if (criteria.hasSetTimeRange()) {
                tableBuilder.setTimeRange(buildTimeRange(criteria.getTimeRange()));
                onlyOneVersionInTimeRange = criteria.getTimeRange().containsOnlyOneVersion();
            }

            // optional int32 max_versions = 6;
            if (criteria.hasSetMaxVersions() && !onlyOneVersionInTimeRange) {
                tableBuilder.setMaxVersions(criteria.getMaxVersions());
            }

            // optional bytes filter = 8;
            if (criteria.hasSetFilter()) {
                tableBuilder.setFilter(buildFilter(criteria.getFilter()));
            }

            // optional string startColumn = 9;
            if (criteria.hasSetStartColumn()) {
                tableBuilder.setStartColumn(criteria.getStartColumn());
            }

            // optional string endColumn = 10;
            if (criteria.hasSetEndColumn()) {
                tableBuilder.setEndColumn(criteria.getEndColumn());
            }

            builder.addTables(tableBuilder.build());
        }
        return builder.build();
    }

    public static OtsInternalApi.BatchWriteRowRequest buildBatchWriteRowRequest(BatchWriteRowRequest request) {
        OtsInternalApi.BatchWriteRowRequest.Builder builder = OtsInternalApi.BatchWriteRowRequest.newBuilder();

        for (String tableName : request.getRowChange().keySet()) {
            OtsInternalApi.TableInBatchWriteRowRequest.Builder tableBuilder = OtsInternalApi.TableInBatchWriteRowRequest.newBuilder();

            tableBuilder.setTableName(tableName);

            List<RowChange> rowChanges = request.getRowChange().get(tableName);
            if (rowChanges != null && !rowChanges.isEmpty()) {
                for (RowChange rowChange : rowChanges) {
                    try {
                        OtsInternalApi.RowInBatchWriteRowRequest.Builder rowBuilder = OtsInternalApi.RowInBatchWriteRowRequest.newBuilder();
                        if (rowChange instanceof RowPutChange) {
                            rowBuilder.setType(OtsInternalApi.OperationType.PUT);
                            rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowPutChangeWithHeader((RowPutChange) rowChange)));
                        } else if (rowChange instanceof RowUpdateChange) {
                            rowBuilder.setType(OtsInternalApi.OperationType.UPDATE);
                            rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowUpdateChangeWithHeader((RowUpdateChange) rowChange)));
                        } else if (rowChange instanceof RowDeleteChange) {
                            rowBuilder.setType(OtsInternalApi.OperationType.DELETE);
                            rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowDeleteChangeWithHeader((RowDeleteChange) rowChange)));
                        } else {
                            throw new ClientException("Unknown type of rowChange.");
                        }
                        rowBuilder.setCondition(buildCondition(rowChange.getCondition()));
                        rowBuilder.setReturnContent(buildReturnContent(rowChange.getReturnType(), rowChange.getReturnColumnNames()));
                        tableBuilder.addRows(rowBuilder.build());
                    } catch (IOException e) {
                        throw new ClientException("Bug: serialize row put change failed.", e);
                    }
                }
            }

            builder.addTables(tableBuilder.build());
        }

        // optional bytes transaction_id = 2;
        if (request.hasSetTransactionId()) {
            builder.setTransactionId(request.getTransactionId());
        }

        if (request.isAtomicSet()) {
            builder.setIsAtomic(request.isAtomic());
        }

        return builder.build();
    }

    public static OtsInternalApi.BulkImportRequest buildBulkImportRequest(BulkImportRequest request) {
        OtsInternalApi.BulkImportRequest.Builder builder = OtsInternalApi.BulkImportRequest.newBuilder();

        builder.setTableName(request.getTableName());

        List<RowChange> rowChanges = request.getRowChange();
        if (rowChanges != null && !rowChanges.isEmpty()) {
            for (RowChange rowChange : rowChanges) {
                try {
                    OtsInternalApi.RowInBulkImportRequest.Builder rowBuilder = OtsInternalApi.RowInBulkImportRequest.newBuilder();
                    if (rowChange instanceof RowPutChange) {
                        rowBuilder.setType(OtsInternalApi.OperationType.PUT);
                        rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowPutChangeWithHeader((RowPutChange) rowChange)));
                    } else if (rowChange instanceof RowUpdateChange) {
                        rowBuilder.setType(OtsInternalApi.OperationType.UPDATE);
                        rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowUpdateChangeWithHeader((RowUpdateChange) rowChange)));
                    } else if (rowChange instanceof RowDeleteChange) {
                        rowBuilder.setType(OtsInternalApi.OperationType.DELETE);
                        rowBuilder.setRowChange(ByteString.copyFrom(PlainBufferBuilder.buildRowDeleteChangeWithHeader((RowDeleteChange) rowChange)));
                    } else {
                        throw new ClientException("Unknown type of rowChange.");
                    }
                    builder.addRows(rowBuilder.build());
                } catch (IOException e) {
                    throw new ClientException("Bug: serialize row put change failed.", e);
                }
            }
        }
        return builder.build();
    }

    public static OtsInternalApi.UpdateTableRequest buildUpdateTableRequest(UpdateTableRequest updateTableRequest) {
        OtsInternalApi.UpdateTableRequest.Builder builder = OtsInternalApi.UpdateTableRequest.newBuilder();
        // required string table_name = 1;
        builder.setTableName(updateTableRequest.getTableName());
        // optional ReservedThroughput reserved_throughput = 2;
        if (updateTableRequest.getReservedThroughputForUpdate() != null) {
            builder.setReservedThroughput(buildReservedThroughput(updateTableRequest.getReservedThroughputForUpdate()));
        }
        // optional TableOptionsEx table_options = 3;
        if (updateTableRequest.getTableOptionsForUpdate() != null) {
            builder.setTableOptions(buildTableOptions(updateTableRequest.getTableOptionsForUpdate()));
        }

        if (updateTableRequest.getStreamSpecification() != null) {
            builder.setStreamSpec(buildStreamSpecification(updateTableRequest.getStreamSpecification()));
        }

        return builder.build();
    }

    private static OtsInternalApi.StreamSpecification buildStreamSpecification(StreamSpecification streamSpecification) {
        OtsInternalApi.StreamSpecification.Builder builder = OtsInternalApi.StreamSpecification.newBuilder();
        builder.setEnableStream(streamSpecification.isEnableStream());
        if (streamSpecification.getExpirationTime() > 0) {
            builder.setExpirationTime(streamSpecification.getExpirationTime());
        }
        if (streamSpecification.isEnableStream()) {
            for (String column : streamSpecification.getOriginColumnsToGet()) {
                builder.addColumnsToGet(column);
            }
        }
        return builder.build();
    }

    private static OtsInternalApi.SSESpecification buildSseSpecification(SSESpecification sseSpecification) {
        OtsInternalApi.SSESpecification.Builder builder = OtsInternalApi.SSESpecification.newBuilder();
        builder.setEnable(sseSpecification.isEnable());
        if (sseSpecification.getKeyType() != null) {
            SSEKeyType keyType = sseSpecification.getKeyType();
            switch (keyType) {
                case SSE_KMS_SERVICE:
                    builder.setKeyType(OtsInternalApi.SSEKeyType.SSE_KMS_SERVICE);
                    break;
                case SSE_BYOK:
                    builder.setKeyType(OtsInternalApi.SSEKeyType.SSE_BYOK);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown server side encryption key type: " + keyType);
            }
        }
        if (sseSpecification.getKeyId() != null) {
            builder.setKeyId(ByteString.copyFrom(sseSpecification.getKeyId().getBytes()));
        }
        if (sseSpecification.getRoleArn() != null) {
            builder.setRoleArn(ByteString.copyFrom(sseSpecification.getRoleArn().getBytes()));
        }
        return builder.build();
    }

    public static OtsFilter.LogicalOperator toLogicalOperator(CompositeColumnValueFilter.LogicOperator type) {
        switch (type) {
            case NOT:
                return OtsFilter.LogicalOperator.LO_NOT;
            case AND:
                return OtsFilter.LogicalOperator.LO_AND;
            case OR:
                return OtsFilter.LogicalOperator.LO_OR;
            default:
                throw new IllegalArgumentException("Unknown logic operation type: " + type);
        }
    }

    public static OtsFilter.Filter toFilter(ColumnCondition f) {
        OtsFilter.Filter.Builder builder = OtsFilter.Filter.newBuilder();
        builder.setType(toFilterType(f.getConditionType()));
        builder.setFilter(f.serialize());
        return builder.build();
    }

    public static OtsFilter.Filter toFilter(Filter f) {
        OtsFilter.Filter.Builder builder = OtsFilter.Filter.newBuilder();
        builder.setType(toFilterType(f.getFilterType()));
        builder.setFilter(f.serialize());
        return builder.build();
    }

    public static OtsFilter.FilterType toFilterType(ColumnConditionType type) {
        switch (type) {
            case COMPOSITE_COLUMN_VALUE_CONDITION:
                return OtsFilter.FilterType.FT_COMPOSITE_COLUMN_VALUE;
            case SINGLE_COLUMN_VALUE_CONDITION:
                return OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE;
            default:
                throw new IllegalArgumentException("Unknown filter type: " + type);
        }
    }

    public static OtsFilter.FilterType toFilterType(FilterType type) {
        switch (type) {
            case COMPOSITE_COLUMN_VALUE_FILTER:
                return OtsFilter.FilterType.FT_COMPOSITE_COLUMN_VALUE;
            case SINGLE_COLUMN_VALUE_FILTER:
                return OtsFilter.FilterType.FT_SINGLE_COLUMN_VALUE;
            case COLUMN_PAGINATION_FILTER:
                return OtsFilter.FilterType.FT_COLUMN_PAGINATION;
            default:
                throw new IllegalArgumentException("Unknown filter type: " + type);
        }
    }

    public static ByteString buildFilter(ColumnCondition filter) {
        return toFilter(filter).toByteString();
    }

    public static ByteString buildFilter(Filter filter) {
        return toFilter(filter).toByteString();
    }

    public static ByteString buildCompositeColumnValueFilter(CompositeColumnValueFilter filter) {
        OtsFilter.CompositeColumnValueFilter.Builder builder = OtsFilter.CompositeColumnValueFilter.newBuilder();
        builder.setCombinator(toLogicalOperator(filter.getOperationType()));

        for (Filter f : filter.getSubFilters()) {
            builder.addSubFilters(toFilter(f));
        }

        return builder.build().toByteString();
    }

    public static ByteString buildSingleColumnValueFilter(SingleColumnValueFilter filter) {
        OtsFilter.SingleColumnValueFilter.Builder builder = OtsFilter.SingleColumnValueFilter.newBuilder();
        builder.setColumnName(filter.getColumnName());
        builder.setComparator(toComparatorType(filter.getOperator()));
        try {
            builder.setColumnValue(ByteString.copyFrom(PlainBufferBuilder.buildColumnValueWithoutLengthPrefix(filter.getColumnValue())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize column value failed.", e);
        }
        builder.setFilterIfMissing(!filter.isPassIfMissing());
        builder.setLatestVersionOnly(filter.isLatestVersionsOnly());

        return builder.build().toByteString();
    }

    public static ByteString buildSingleColumnValueRegexFilter(SingleColumnValueRegexFilter filter) {
        OtsFilter.SingleColumnValueFilter.Builder builder = OtsFilter.SingleColumnValueFilter.newBuilder();
        builder.setColumnName(filter.getColumnName());
        if (filter.hasRegexRule()) {
            builder.setValueTransRule(filter.getRegexRule().serialize());
        }
        builder.setComparator(toComparatorType(filter.getOperator()));
        try {
            builder.setColumnValue(ByteString.copyFrom(PlainBufferBuilder.buildColumnValueWithoutLengthPrefix(filter.getColumnValue())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize column value failed.", e);
        }
        builder.setFilterIfMissing(true);
        builder.setLatestVersionOnly(filter.getLatestVersionsOnly());
        return builder.build().toByteString();
    }

    private static OtsFilter.ComparatorType toComparatorType(SingleColumnValueRegexFilter.CompareOperator operator) {
        switch (operator) {
            case EQUAL:
                return OtsFilter.ComparatorType.CT_EQUAL;
            case NOT_EQUAL:
                return OtsFilter.ComparatorType.CT_NOT_EQUAL;
            case GREATER_THAN:
                return OtsFilter.ComparatorType.CT_GREATER_THAN;
            case GREATER_EQUAL:
                return OtsFilter.ComparatorType.CT_GREATER_EQUAL;
            case LESS_THAN:
                return OtsFilter.ComparatorType.CT_LESS_THAN;
            case LESS_EQUAL:
                return OtsFilter.ComparatorType.CT_LESS_EQUAL;
            case EXIST:
                return OtsFilter.ComparatorType.CT_EXIST;
            case NOT_EXIST:
                return OtsFilter.ComparatorType.CT_NOT_EXIST;
            default:
                throw new IllegalArgumentException("Unknown compare operator: " + operator);
        }
    }

    private static OtsFilter.ComparatorType toComparatorType(SingleColumnValueFilter.CompareOperator operator) {
        switch (operator) {
            case EQUAL:
                return OtsFilter.ComparatorType.CT_EQUAL;
            case NOT_EQUAL:
                return OtsFilter.ComparatorType.CT_NOT_EQUAL;
            case GREATER_THAN:
                return OtsFilter.ComparatorType.CT_GREATER_THAN;
            case GREATER_EQUAL:
                return OtsFilter.ComparatorType.CT_GREATER_EQUAL;
            case LESS_THAN:
                return OtsFilter.ComparatorType.CT_LESS_THAN;
            case LESS_EQUAL:
                return OtsFilter.ComparatorType.CT_LESS_EQUAL;
            default:
                throw new IllegalArgumentException("Unknown compare operator: " + operator);
        }
    }

    public static ByteString buildColumnPaginationFilter(ColumnPaginationFilter filter) {
        OtsFilter.ColumnPaginationFilter.Builder builder = OtsFilter.ColumnPaginationFilter.newBuilder();
        builder.setLimit(filter.getLimit());
        builder.setOffset(filter.getOffset());
        return builder.build().toByteString();
    }

    public static Message buildListStreamRequest(ListStreamRequest request) {
        OtsInternalApi.ListStreamRequest.Builder builder = OtsInternalApi.ListStreamRequest.newBuilder();
        if (request.getTableName() != null) {
            builder.setTableName(request.getTableName());
        }
        return builder.build();
    }

    public static OtsInternalApi.DescribeStreamRequest buildDescribeStreamRequest(DescribeStreamRequest request) {
        OtsInternalApi.DescribeStreamRequest.Builder builder = OtsInternalApi.DescribeStreamRequest.newBuilder();
        builder.setStreamId(request.getStreamId());
        if (request.getInclusiveStartShardId() != null) {
            builder.setInclusiveStartShardId(request.getInclusiveStartShardId());
        }
        if (request.getShardLimit() > 0) {
            builder.setShardLimit(request.getShardLimit());
        }
        if (request.isSupportTimeseriesTable()) {
            builder.setSupportTimeseriesDataTable(true);
        }
        return builder.build();
    }

    public static OtsInternalApi.GetShardIteratorRequest buildGetShardIteratorRequest(GetShardIteratorRequest request) {
        OtsInternalApi.GetShardIteratorRequest.Builder builder = OtsInternalApi.GetShardIteratorRequest.newBuilder();
        builder.setStreamId(request.getStreamId());
        builder.setShardId(request.getShardId());
        if (request.hasTimestamp()) {
            builder.setTimestamp(request.getTimestamp());
        }
        if (request.hasToken()) {
            builder.setToken(request.getToken());
        }
        return builder.build();
    }

    public static OtsInternalApi.GetStreamRecordRequest buildGetStreamRecordRequest(GetStreamRecordRequest request) {
        OtsInternalApi.GetStreamRecordRequest.Builder builder = OtsInternalApi.GetStreamRecordRequest.newBuilder();
        builder.setShardIterator(request.getShardIterator());
        if (request.getLimit() > 0) {
            builder.setLimit(request.getLimit());
        }
        if (request.getTableName() != null && !request.getTableName().isEmpty()) {
            builder.setTableName(request.getTableName());
        }
        return builder.build();
    }

    public static OtsInternalApi.AbortTransactionRequest buildAbortTransactionRequest(AbortTransactionRequest request) {
        OtsInternalApi.AbortTransactionRequest.Builder builder = OtsInternalApi.AbortTransactionRequest.newBuilder();
        builder.setTransactionId(request.getTransactionID());
        return builder.build();
    }

    public static OtsInternalApi.StartLocalTransactionRequest buildStartLocalTransactionRequest(StartLocalTransactionRequest request) {
        OtsInternalApi.StartLocalTransactionRequest.Builder builder = OtsInternalApi.StartLocalTransactionRequest.newBuilder();
        builder.setTableName(request.getTableName());
        try {
            builder.setKey(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(request.getPrimaryKey())));
        } catch (IOException e) {
            throw new ClientException("Bug: serialize StartLocalTransactionRequest failed.", e);
       }
        return builder.build();
    }

    public static OtsInternalApi.CommitTransactionRequest buildCommitTransactionRequest(CommitTransactionRequest request) {
        OtsInternalApi.CommitTransactionRequest.Builder builder = OtsInternalApi.CommitTransactionRequest.newBuilder();
        builder.setTransactionId(request.getTransactionID());
        return builder.build();
    }

    public static OtsInternalApi.ComputeSplitsRequest buildComputeSplitsRequest(ComputeSplitsRequest request) {
        OtsInternalApi.ComputeSplitsRequest.Builder builder = OtsInternalApi.ComputeSplitsRequest.newBuilder();
        if (null != request.getTableName()) {
            builder.setTableName(request.getTableName());
        }
        if (null != request.getSplitsOptions()) {
            SplitsOptions splitsOptions = request.getSplitsOptions();
            if (splitsOptions instanceof SearchIndexSplitsOptions) {
                SearchIndexSplitsOptions searchIndexSplitsOptions = (SearchIndexSplitsOptions)splitsOptions;
                OtsInternalApi.SearchIndexSplitsOptions.Builder optionsBuilder = OtsInternalApi.SearchIndexSplitsOptions.newBuilder();
                if (null != searchIndexSplitsOptions.getIndexName()) {
                    optionsBuilder.setIndexName(searchIndexSplitsOptions.getIndexName());
                }
                builder.setSearchIndexSplitsOptions(optionsBuilder.build());
            }
        }
        return builder.build();
    }

    public static OtsInternalApi.SQLQueryRequest buildSQLQueryRequest(SQLQueryRequest request) {
        OtsInternalApi.SQLQueryRequest.Builder builder = OtsInternalApi.SQLQueryRequest.newBuilder();

        builder.setQuery(request.getQuery());
        builder.setVersion(buildSQLPayloadVersion(request.getSqlPayloadVersion()));
        builder.setSqlVersion(1);

        return builder.build();
    }

    public static OtsInternalApi.SQLPayloadVersion buildSQLPayloadVersion(SQLPayloadVersion sqlPayloadVersion) {
        switch (sqlPayloadVersion) {
            case SQL_FLAT_BUFFERS:
                return OtsInternalApi.SQLPayloadVersion.SQL_FLAT_BUFFERS;
            default:
                throw new UnsupportedOperationException("not supported sql payload version: " + sqlPayloadVersion);
        }
    }
}
