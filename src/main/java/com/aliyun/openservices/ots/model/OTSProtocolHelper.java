/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.model.condition.ColumnCondition;
import com.aliyun.openservices.ots.model.condition.ColumnConditionType;
import com.aliyun.openservices.ots.protocol.OtsProtocol2;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.ConsumedCapacity;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.*;
import com.google.protobuf.ByteString;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;

public class OTSProtocolHelper {

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType toPBColumnType(PrimaryKeyType pkType) {
        switch(pkType) {
        case INTEGER:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER;
        case STRING:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING;
        case BINARY:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidPrimaryKeyType", pkType.toString()));
        }
    }

    public static PrimaryKeyType toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType type) {
        switch(type) {
        case INTEGER:
            return PrimaryKeyType.INTEGER;
        case STRING:
            return PrimaryKeyType.STRING;
        case BINARY:
            return PrimaryKeyType.BINARY;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidPrimaryKeyType", type.toString()));
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType toPBColumnType(com.aliyun.openservices.ots.model.ColumnType colType) {
        switch(colType) {
        case BOOLEAN:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN;
        case INTEGER:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER;
        case STRING:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING;
        case DOUBLE:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE;
        case BINARY:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidColumnType", colType.toString()));
        }
    }

    public static com.aliyun.openservices.ots.model.ColumnType toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType type) {
        switch(type) {
        case BOOLEAN:
            return com.aliyun.openservices.ots.model.ColumnType.BOOLEAN;
        case DOUBLE:
            return com.aliyun.openservices.ots.model.ColumnType.DOUBLE;
        case INTEGER:
            return com.aliyun.openservices.ots.model.ColumnType.INTEGER;
        case STRING:
            return com.aliyun.openservices.ots.model.ColumnType.STRING;
        case BINARY:
            return com.aliyun.openservices.ots.model.ColumnType.BINARY;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidColumnType", type.toString()));
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue buildColumnValue(com.aliyun.openservices.ots.model.PrimaryKeyValue primaryKey) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.newBuilder();
        switch(primaryKey.getType()){
        case INTEGER:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
            // optional int64 v_int = 2;
            builder.setVInt(primaryKey.asLong());
            break;
        case STRING:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
            // optional string v_string = 3;
            builder.setVString(primaryKey.asString());
            break;
        case BINARY:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY);
            // optional string v_binary = 4;
            builder.setVBinary(ByteString.copyFrom(primaryKey.asBinary()));
            break;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidPartitionKeyType", primaryKey.getType().toString()));
        }
        return builder.build();
    }

    public static PrimaryKeyValue toPrimaryKeyValue(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue value) {
        switch(value.getType()) {
        case INTEGER:
            if (!value.hasVInt()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_int"));
            }
            return PrimaryKeyValue.fromLong(value.getVInt());
        case STRING:
            if (!value.hasVString()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_string"));
            }
            return PrimaryKeyValue.fromString(value.getVString());
        case BINARY:
            if (!value.hasVBinary()) {
               throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_binary"));
            }
            return PrimaryKeyValue.fromBinary(value.getVBinary().toByteArray());
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidPrimaryKeyType", value.getType().toString()));
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue buildColumnValue(com.aliyun.openservices.ots.model.ColumnValue column) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.newBuilder();
        switch(column.getType()){
        case INTEGER:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
            // optional int64 v_int = 2;
            builder.setVInt(column.asLong());
            break;
        case STRING:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
            // optional string v_string = 3;
            builder.setVString(column.asString());
            break;
        case BOOLEAN:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN);
            // optional bool v_bool = 4;
            builder.setVBool(column.asBoolean());
            break;
        case DOUBLE:
            // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE);
            // optional double v_double = 5;
            builder.setVDouble(column.asDouble());
            break;
        case BINARY:
         // required ColumnType type = 1;
            builder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY);
            // optional double v_binary = 5;
            builder.setVBinary(ByteString.copyFrom(column.asBinary()));
            break;
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidColumnType", column.getType().toString()));
        }
        return builder.build();
    }

    public static com.aliyun.openservices.ots.model.ColumnValue toColumnValue(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue value) {
        switch(value.getType()) {
        case BOOLEAN:
            if(!value.hasVBool()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_bool"));
            }
            return com.aliyun.openservices.ots.model.ColumnValue.fromBoolean(value.getVBool());
        case INTEGER:
            if(!value.hasVInt()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_int"));
            }
            return com.aliyun.openservices.ots.model.ColumnValue.fromLong(value.getVInt());
        case STRING:
            if(!value.hasVString()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_string"));
            }
            return com.aliyun.openservices.ots.model.ColumnValue.fromString(value.getVString());
        case DOUBLE:
            if(!value.hasVDouble()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_double"));
            }
            return com.aliyun.openservices.ots.model.ColumnValue.fromDouble(value.getVDouble());
        case BINARY:
            if(!value.hasVBinary()) {
                throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("ValueNotSet", "ColumnValue.v_binary"));
            }
            return com.aliyun.openservices.ots.model.ColumnValue.fromBinary(value.getVBinary().toByteArray());
        default:
            throw new IllegalArgumentException(OTS_RESOURCE_MANAGER.getFormattedString("InvalidColumnType", value.getType().toString()));
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation toPBRowExistenceExpectation(
            com.aliyun.openservices.ots.model.RowExistenceExpectation rowExistenceExpectation) {
        switch(rowExistenceExpectation) {
        case EXPECT_EXIST:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.EXPECT_EXIST;
        case EXPECT_NOT_EXIST:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.EXPECT_NOT_EXIST;
        case IGNORE:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.IGNORE;
        default:
            throw new IllegalArgumentException("Invalid row existence expectation: " + rowExistenceExpectation);
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnConditionType toPBColumnConditionType(ColumnConditionType cct)
    {
        switch (cct) {
            case RELATIONAL_CONDITION:
                return OtsProtocol2.ColumnConditionType.CCT_RELATION;
            case COMPOSITE_CONDITION:
                return OtsProtocol2.ColumnConditionType.CCT_COMPOSITE;
            default:
                throw new IllegalArgumentException("Invalid column condition type: " + cct);
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnCondition buildColumnCondition(ColumnCondition cc)
    {
        OtsProtocol2.ColumnCondition.Builder builder = OtsProtocol2.ColumnCondition.newBuilder();
        builder.setType(toPBColumnConditionType(cc.getType()));
        builder.setCondition(cc.serialize());

        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Condition buildCondition(Condition cond)
    {
        OtsProtocol2.Condition.Builder builder = OtsProtocol2.Condition.newBuilder();
        builder.setRowExistence(toPBRowExistenceExpectation(cond.getRowExistenceExpectation()));
        if (cond.getColumnCondition() != null) {
            builder.setColumnCondition(buildColumnCondition(cond.getColumnCondition()));
        }

        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Column buildColumn(String name, PrimaryKeyValue value) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.newBuilder();
        // required string name = 1;
        builder.setName(name);
        // required ColumnValue value = 2;
        builder.setValue(buildColumnValue(value));
        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Column buildColumn(String name, com.aliyun.openservices.ots.model.ColumnValue value) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.newBuilder();
        // required string name = 1;
        builder.setName(name);
        // required ColumnValue value = 2;
        builder.setValue(buildColumnValue(value));
        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnSchema buildColumnSchema(String name, PrimaryKeyType type) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnSchema.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnSchema.newBuilder();
        // required string name = 1;
        builder.setName(name);
        // required ColumnType type = 2;
        builder.setType(toPBColumnType(type));
        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.TableMeta buildTableMeta(
            TableMeta tableMeta) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.TableMeta.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.TableMeta.newBuilder();
        // required string table_name = 1;
        builder.setTableName(tableMeta.getTableName());
        
        // repeated ColumnSchema primary_keys = 2;
        for (Entry<String, PrimaryKeyType> entry : tableMeta.getPrimaryKey().entrySet()) {
            builder.addPrimaryKey(buildColumnSchema(entry.getKey(), entry.getValue()));
        }
        
        return builder.build();
    }
    
    public static OtsProtocol2.GetRowRequest buildGetRowRequest(SingleRowQueryCriteria criteria) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRowRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRowRequest.newBuilder();
        // required string table_name = 1;
        builder.setTableName(criteria.getTableName());
        
        // repeated Column primary_keys = 2;
        for (Entry<String, PrimaryKeyValue> entry : criteria.getRowPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addPrimaryKey(buildColumn(entry.getKey(), entry.getValue()));
        }

        // repeated string columns_to_get = 3;
        for (String name : criteria.getColumnsToGet()) {
            builder.addColumnsToGet(name);
        }

        // optional ColumnCondition filter = 4;
        if (criteria.getFilter() != null) {
            builder.setFilter(buildColumnCondition(criteria.getFilter()));
        }

        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Row buildRow(
            Map<String, PrimaryKeyValue> primaryKey,
            Map<String, com.aliyun.openservices.ots.model.ColumnValue> attributeColumns) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.Row.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.Row.newBuilder();

        for (Entry<String, PrimaryKeyValue> entry : primaryKey.entrySet()) {
            builder.addPrimaryKeyColumns(buildColumn(entry.getKey(), entry.getValue()));
        }

        for (Entry<String, com.aliyun.openservices.ots.model.ColumnValue> entry : attributeColumns.entrySet()) {
            builder.addAttributeColumns(buildColumn(entry.getKey(), entry.getValue()));
        }

        return builder.build();
    }

    public static OtsProtocol2.CreateTableRequest buildCreateTableRequest(CreateTableRequest request) {
        TableMeta tableMeta = request.getTableMeta();
        ReservedThroughput reservedThroughput = request.getReservedThroughput();
        com.aliyun.openservices.ots.protocol.OtsProtocol2.CreateTableRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.CreateTableRequest.newBuilder();
        
        // required TableMeta table_meta = 1;
        builder.setTableMeta(buildTableMeta(tableMeta));
        
        builder.setReservedThroughput(buildReservedThroughput(reservedThroughput));

        return builder.build();
    }

    private static com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughput buildReservedThroughput(
            com.aliyun.openservices.ots.model.ReservedThroughput reservedThroughput) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughput.Builder rtBuilder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughput.newBuilder();

        com.aliyun.openservices.ots.model.CapacityUnit capacityUnit = reservedThroughput.getCapacityUnit();
        com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit.newBuilder();

        builder.setRead(capacityUnit.getReadCapacityUnit());
        builder.setWrite(capacityUnit.getWriteCapacityUnit());
        rtBuilder.setCapacityUnit(builder.build());
        return rtBuilder.build();
    }

    public static OtsProtocol2.DeleteTableRequest buildDeleteTableRequest(String tableName) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteTableRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteTableRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(tableName);
        return builder.build();
    }

    public static OtsProtocol2.DeleteRowRequest buildDeleteRowRequest(RowDeleteChange rowChange) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteRowRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteRowRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());

        // required Condition condition = 2;
        builder.setCondition(buildCondition(rowChange.getCondition()));

        // repeated Column primary_keys = 3;
        for (Entry<String, PrimaryKeyValue> entry : rowChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addPrimaryKey(buildColumn(entry.getKey(), entry.getValue()));
        }

        return builder.build();
    }

    public static OtsProtocol2.PutRowRequest buildPutRowRequest(RowPutChange rowChange) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.PutRowRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.PutRowRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());

        // required Condition condition = 2;
        builder.setCondition(buildCondition(rowChange.getCondition()));

        // repeated Column primary_keys = 3;
        for (Entry<String, PrimaryKeyValue> entry : rowChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addPrimaryKey(buildColumn(entry.getKey(), entry.getValue()));
        }

        // repeated Column columns = 4;
        for (Entry<String, ColumnValue> entry : rowChange.getAttributeColumns().entrySet()) {
            builder.addAttributeColumns(buildColumn(entry.getKey(), entry.getValue()));
        }

        return builder.build();
    }

    public static ReservedThroughputDetails parseCapacityUnitDetails(com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughputDetails capacityUnitDetails) {
        ReservedThroughputDetails details = new ReservedThroughputDetails();

        com.aliyun.openservices.ots.model.CapacityUnit tableCU = new com.aliyun.openservices.ots.model.CapacityUnit();
        com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit capacityUnit = capacityUnitDetails.getCapacityUnit();
        if (capacityUnit.hasRead()) {
            tableCU.setReadCapacityUnit(capacityUnit.getRead());
        }

        if (capacityUnit.hasWrite()) {
            tableCU.setWriteCapacityUnit(capacityUnit.getWrite());
        }

        details.setCapacityUnit(tableCU);
        details.setLastIncreaseTime(capacityUnitDetails.getLastIncreaseTime());
        details.setNumberOfDecreasesToday(capacityUnitDetails.getNumberOfDecreasesToday());
        if (capacityUnitDetails.hasLastDecreaseTime()) {
            details.setLastDecreaseTime(capacityUnitDetails.getLastDecreaseTime());
        }
        return details;
    }

    public static TableMeta parseTableMeta(com.aliyun.openservices.ots.protocol.OtsProtocol2.TableMeta meta) {
        TableMeta tableMeta = new TableMeta(meta.getTableName());

        for (com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnSchema pk : meta.getPrimaryKeyList()) {
            tableMeta.addPrimaryKeyColumn(pk.getName(), toPrimaryKeyType(pk.getType()));
        }

        return tableMeta;
    }

    public static Row parseRow(com.aliyun.openservices.ots.protocol.OtsProtocol2.Row pbRow) {
        Row row = new Row();
        for (com.aliyun.openservices.ots.protocol.OtsProtocol2.Column pk : pbRow.getPrimaryKeyColumnsList()) {
            row.addColumn(pk.getName(), toColumnValue(pk.getValue()));
        }

        for (com.aliyun.openservices.ots.protocol.OtsProtocol2.Column col : pbRow.getAttributeColumnsList()) {
            row.addColumn(col.getName(), toColumnValue(col.getValue()));
        }
        return row;
    }

    public static Row parseGetRowResponse(com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRowResponse response) {
        return parseRow(response.getRow());
    }
    
    public static OtsProtocol2.ListTableRequest buildListTableRequest() {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ListTableRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ListTableRequest.newBuilder();
        return builder.build();
    }

    public static OtsProtocol2.DescribeTableRequest buildDescribeTableRequest(String tableName) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.DescribeTableRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.DescribeTableRequest.newBuilder();
        
        // required string table_name = 1;
        builder.setTableName(tableName);
        
        return builder.build();
    }
    
    public static OtsProtocol2.UpdateTableRequest buildUpdateTableRequest(UpdateTableRequest request) {
        String tableName = request.getTableName();
        ReservedThroughputChange rtChange = request.getReservedThrougputChange();

        com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateTableRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateTableRequest.newBuilder();
        
        // required string table_name = 1;
        builder.setTableName(tableName);
        
        // optional CapacityUnit capacity_unit = 2;
        if (rtChange != null) {
            com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughput.Builder rtBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughput.newBuilder();
            com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit.Builder cuBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit.newBuilder();

            if (rtChange.isReadSet()) {
                cuBuilder.setRead(rtChange.getReadCapacityUnit());
            }

            if (rtChange.isWriteSet()) {
                cuBuilder.setWrite(rtChange.getWriteCapacityUnit());
            }

            rtBuilder.setCapacityUnit(cuBuilder.build());
            builder.setReservedThroughput(rtBuilder.build());
        }

        return builder.build();
    }

    public static OtsProtocol2.UpdateRowRequest buildUpdateRowRequest(RowUpdateChange rowChange) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateRowRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateRowRequest.newBuilder();

        // required string table_name = 1;
        builder.setTableName(rowChange.getTableName());

        // required Condition condition = 2;
        builder.setCondition(buildCondition(rowChange.getCondition()));

        // repeated Column primary_keys = 3;
        for (Entry<String, PrimaryKeyValue> entry : rowChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addPrimaryKey(buildColumn(entry.getKey(), entry.getValue()));
        }

        // repeated ColumnUpdate columns = 4; 
        for (Entry<String, ColumnValue> entry : rowChange.getAttributeColumns().entrySet()) {
            com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnUpdate.Builder columnUpdateBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnUpdate.newBuilder();
            columnUpdateBuilder.setName(entry.getKey());
            if (entry.getValue() != null) {
                columnUpdateBuilder.setType(OperationType.PUT);
                columnUpdateBuilder.setValue(buildColumnValue(entry.getValue()));
            } else {
                columnUpdateBuilder.setType(OperationType.DELETE);
            }
            builder.addAttributeColumns(columnUpdateBuilder.build());
        }

        return builder.build();
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Direction toPBDirection(Direction direction) {
        switch (direction) {
        case BACKWARD:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.Direction.BACKWARD;
        case FORWARD:
            return com.aliyun.openservices.ots.protocol.OtsProtocol2.Direction.FORWARD;
        default:
            throw new IllegalArgumentException("Invalid direction type: " + direction);
        }
    }

    public static com.aliyun.openservices.ots.protocol.OtsProtocol2.Column buildRangeColumn(String name, PrimaryKeyValue value) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.Column.newBuilder();
        builder.setName(name);

        if(value == PrimaryKeyValue.INF_MIN) {
            com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.Builder valueBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.newBuilder();
            valueBuilder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MIN);
            builder.setValue(valueBuilder.build());
        } else if(value == PrimaryKeyValue.INF_MAX) {
            com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.Builder valueBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue.newBuilder();
            valueBuilder.setType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MAX);
            builder.setValue(valueBuilder.build());
        } else {
            builder.setValue(buildColumnValue(value));
        }

        return builder.build();
    }

    public static OtsProtocol2.GetRangeRequest buildGetRangeRequest(RangeRowQueryCriteria criteria) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRangeRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.GetRangeRequest.newBuilder();

        //required string table_name = 1;
        builder.setTableName(criteria.getTableName());

        //required Direction direction = 2;
        builder.setDirection(toPBDirection(criteria.getDirection()));

        //repeated string columns_to_get = 3;
        for (String column : criteria.getColumnsToGet()) {
            builder.addColumnsToGet(column);
        }

        //optional uint32 limit = 4;
        if (criteria.getLimit() >= 0) {
            builder.setLimit(criteria.getLimit());
        }

        //repeated Column inclusive_start_primary_keys = 5; 
        for (Entry<String, PrimaryKeyValue> column : criteria.getInclusiveStartPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addInclusiveStartPrimaryKey(buildRangeColumn(column.getKey(), column.getValue()));
        }

        //repeated Column exclusive_end_primary_keys = 6; 
        for (Entry<String, PrimaryKeyValue> column : criteria.getExclusiveEndPrimaryKey().getPrimaryKey().entrySet()) {
            builder.addExclusiveEndPrimaryKey(buildRangeColumn(column.getKey(), column.getValue()));
        }

        //optional ColumnCondition filter = 7;
        if (criteria.getFilter() != null) {
            builder.setFilter(buildColumnCondition(criteria.getFilter()));
        }
        return builder.build();
    }

    public static OtsProtocol2.BatchGetRowRequest buildBatchGetRowRequest(
            Map<String, MultiRowQueryCriteria> criteriasGroupByTable) {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.BatchGetRowRequest.Builder builder = 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.BatchGetRowRequest.newBuilder();
        
        for (Entry<String, MultiRowQueryCriteria> entry : criteriasGroupByTable.entrySet()) {
            String tableName = entry.getKey();
            MultiRowQueryCriteria criteria = entry.getValue();
            com.aliyun.openservices.ots.protocol.OtsProtocol2.TableInBatchGetRowRequest.Builder tableBuilder = 
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.TableInBatchGetRowRequest.newBuilder();
            tableBuilder.setTableName(tableName);
            for (RowPrimaryKey primaryKey : criteria.getRowKeys()) {
                com.aliyun.openservices.ots.protocol.OtsProtocol2.RowInBatchGetRowRequest.Builder rowBuilder = 
                        com.aliyun.openservices.ots.protocol.OtsProtocol2.RowInBatchGetRowRequest.newBuilder();
                for (Entry<String, PrimaryKeyValue> key : primaryKey.getPrimaryKey().entrySet()) {
                    rowBuilder.addPrimaryKey(buildColumn(key.getKey(), key.getValue()));
                }
                
                tableBuilder.addRows(rowBuilder.build());
            }
            
            for (String column : criteria.getColumnsToGet()) {
                tableBuilder.addColumnsToGet(column);
            }

            if (criteria.getFilter() != null) {
                tableBuilder.setFilter(buildColumnCondition(criteria.getFilter()));
            }
            builder.addTables(tableBuilder.build());
        }
        
        return builder.build();
    }

    public static OtsProtocol2.BatchWriteRowRequest buildBatchWriteRowRequest(BatchWriteRowRequest request) {
        Map<String, List<RowPutChange>> rowPutChangesGroupByTable = request.getRowPutChange();
        Map<String, List<RowUpdateChange>> rowUpdateChangesGroupByTable = request.getRowUpdateChange();
        Map<String, List<RowDeleteChange>> rowDeleteChangesGroupByTable = request.getRowDeleteChange();

        Set<String> allTables = new HashSet<String>();
        allTables.addAll(rowPutChangesGroupByTable.keySet());
        allTables.addAll(rowUpdateChangesGroupByTable.keySet());
        allTables.addAll(rowDeleteChangesGroupByTable.keySet());

        com.aliyun.openservices.ots.protocol.OtsProtocol2.BatchWriteRowRequest.Builder builder =
                com.aliyun.openservices.ots.protocol.OtsProtocol2.BatchWriteRowRequest.newBuilder();

        for (String tableName : allTables) {
            com.aliyun.openservices.ots.protocol.OtsProtocol2.TableInBatchWriteRowRequest.Builder tableBuilder =
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.TableInBatchWriteRowRequest.newBuilder();

            tableBuilder.setTableName(tableName);

            List<RowPutChange> rowPutChanges = rowPutChangesGroupByTable.get(tableName);
            if (rowPutChanges != null) {
                for (RowPutChange rowPutChange : rowPutChanges) {
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.PutRowInBatchWriteRowRequest.Builder putRowBuilder =
                            com.aliyun.openservices.ots.protocol.OtsProtocol2.PutRowInBatchWriteRowRequest.newBuilder();

                    putRowBuilder.setCondition(buildCondition(rowPutChange.getCondition()));

                    for (Entry<String, PrimaryKeyValue> primaryKey : rowPutChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                        putRowBuilder.addPrimaryKey(buildColumn(primaryKey.getKey(), primaryKey.getValue()));
                    }

                    for (Entry<String, ColumnValue> column : rowPutChange.getAttributeColumns().entrySet()) {
                        putRowBuilder.addAttributeColumns(buildColumn(column.getKey(), column.getValue()));
                    }

                    tableBuilder.addPutRows(putRowBuilder.build());
                }
            }

            List<RowUpdateChange> rowUpdateChanges = rowUpdateChangesGroupByTable.get(tableName);
            if (rowUpdateChanges != null) {
                for (RowUpdateChange rowUpdateChange : rowUpdateChanges) {
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateRowInBatchWriteRowRequest.Builder updateRowBuilder =
                            com.aliyun.openservices.ots.protocol.OtsProtocol2.UpdateRowInBatchWriteRowRequest.newBuilder();

                    updateRowBuilder.setCondition(buildCondition(rowUpdateChange.getCondition()));

                    for (Entry<String, PrimaryKeyValue> primaryKey : rowUpdateChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                        updateRowBuilder.addPrimaryKey(buildColumn(primaryKey.getKey(), primaryKey.getValue()));
                    }

                    for (Entry<String, ColumnValue> column : rowUpdateChange.getAttributeColumns().entrySet()) {
                        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnUpdate.Builder columnUpdateBuilder =
                                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnUpdate.newBuilder();
                        columnUpdateBuilder.setName(column.getKey());
                        if (column.getValue() != null) {
                            columnUpdateBuilder.setType(OperationType.PUT);
                            columnUpdateBuilder.setValue(buildColumnValue(column.getValue()));
                        } else {
                            columnUpdateBuilder.setType(OperationType.DELETE);
                        }
                        updateRowBuilder.addAttributeColumns(columnUpdateBuilder.build());
                    }

                    tableBuilder.addUpdateRows(updateRowBuilder.build());
                }
            }

            List<RowDeleteChange> rowDeleteChanges = rowDeleteChangesGroupByTable.get(tableName);
            if (rowDeleteChanges != null) {
                for (RowDeleteChange rowDeleteChange : rowDeleteChanges) {
                    com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteRowInBatchWriteRowRequest.Builder deleteRowBuilder =
                            com.aliyun.openservices.ots.protocol.OtsProtocol2.DeleteRowInBatchWriteRowRequest.newBuilder();

                    deleteRowBuilder.setCondition(buildCondition(rowDeleteChange.getCondition()));

                    for (Entry<String, PrimaryKeyValue> primaryKey : rowDeleteChange.getRowPrimaryKey().getPrimaryKey().entrySet()) {
                        deleteRowBuilder.addPrimaryKey(buildColumn(primaryKey.getKey(), primaryKey.getValue()));
                    }

                    tableBuilder.addDeleteRows(deleteRowBuilder.build());
                }
            }

            builder.addTables(tableBuilder.build());
        }

        return builder.build();
    }

    public static com.aliyun.openservices.ots.model.ConsumedCapacity parseConsumedCapacity(ConsumedCapacity consumed) {
        com.aliyun.openservices.ots.model.ConsumedCapacity cc = new com.aliyun.openservices.ots.model.ConsumedCapacity();
        com.aliyun.openservices.ots.model.CapacityUnit cu = new com.aliyun.openservices.ots.model.CapacityUnit();
        CapacityUnit capacityUnit = consumed.getCapacityUnit();
        if (capacityUnit.hasRead()) {
            cu.setReadCapacityUnit(capacityUnit.getRead());
        }

        if (capacityUnit.hasWrite()) {
            cu.setWriteCapacityUnit(capacityUnit.getWrite());
        }

        cc.setCapacityUnit(cu);

        return cc;
    }

    public static com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus parseBatchGetRowStatus(String tableName, RowInBatchGetRowResponse status, int index) {
        if (status.getIsOk()) {
            Row row = parseRow(status.getRow());
            com.aliyun.openservices.ots.model.ConsumedCapacity cc = parseConsumedCapacity(status.getConsumed());
            return new com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus(tableName, row, cc, index);
        } else {
            Error error = new Error(status.getError().getCode(), status.getError().getMessage());
            return new com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus(tableName, error, index);
        }
    }

    public static com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus parseBatchWriteRowStatus(String tableName, RowInBatchWriteRowResponse status, int index) {
        if (status.getIsOk()) {
            com.aliyun.openservices.ots.model.ConsumedCapacity cc = parseConsumedCapacity(status.getConsumed());
            return new com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus(tableName, cc, index);
        } else {
            Error error = new Error(status.getError().getCode(), status.getError().getMessage());
            return new com.aliyun.openservices.ots.model.BatchWriteRowResult.RowStatus(tableName, error, index);
        }
    }

}
