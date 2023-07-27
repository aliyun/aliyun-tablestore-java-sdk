package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.BytesValue;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ColumnValues;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.DataType;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.RLEStringValues;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumn;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumns;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.LogicType;
import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.ComplexColumnTypeInfo;
import com.alicloud.openservices.tablestore.model.ColumnType;

import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表示以 Flatbuffers 协议列式存储的数据集
 **/
public class SQLRowsFBsColumnBased implements SQLRows {

    private SQLTableMeta sqlTableMeta;

    private String[] columnNames;

    private byte[] columnTypes;

    private ColumnValues[] columnValues;

    private RLEStringValues[] rleStringValues;

    private ComplexColumnTypeInfo[] columnComplexTypeInfos;

    private long rowCount;

    private long columnCount;

    public SQLRowsFBsColumnBased(SQLResponseColumns columns) {
        this.columnNames = new String[columns.columnsLength()];
        this.columnTypes = new byte[columns.columnsLength()];
        this.columnValues = new ColumnValues[columns.columnsLength()];
        this.rleStringValues = new RLEStringValues[columns.columnsLength()];
        this.columnComplexTypeInfos = new ComplexColumnTypeInfo[columns.columnsLength()];
        for (int i = 0; i < columns.columnsLength(); i++) {
            SQLResponseColumn column = columns.columns(i);
            this.columnNames[i] = column.columnName();
            this.columnTypes[i] = column.columnType();
            this.columnValues[i] = column.columnValue();
            this.rleStringValues[i] = this.columnValues[i].rleStringValues();
            this.columnComplexTypeInfos[i] = column.columnComplexTypeInfo();
        }
        this.rowCount = columns.rowCount();
        this.columnCount = columns.columnsLength();
        this.sqlTableMeta = resolveSQLTableMetaFromColumns();
    }

    private SQLTableMeta resolveSQLTableMetaFromColumns() {
        List<SQLColumnSchema> schema = new ArrayList<SQLColumnSchema>();
        Map<String, Integer> columnsMap = new HashMap<String, Integer>();
        for (int i = 0; i < columnCount; i++) {
            schema.add(new SQLColumnSchema(columnNames[i], convertColumnType(columnTypes[i],this.columnComplexTypeInfos[i].columnLogicType())));
            columnsMap.put(columnNames[i], i);
        }
        return new SQLTableMeta(schema, columnsMap);
    }

    private ColumnType convertColumnType(byte columnType,byte logicType) {
        switch (columnType) {
            case DataType.LONG:
                return ColumnType.INTEGER;
            case DataType.BOOLEAN:
                return ColumnType.BOOLEAN;
            case DataType.DOUBLE:
                return ColumnType.DOUBLE;
            case DataType.STRING:
            case DataType.STRING_RLE:
                return ColumnType.STRING;
            case DataType.BINARY:
                return ColumnType.BINARY;
            case DataType.COMPLEX:
                switch (logicType) {
                	case LogicType.DATETIME:
                			return ColumnType.DATETIME;
                	case LogicType.TIME:
                			return ColumnType.TIME;
                	case LogicType.DATE:
                			return ColumnType.DATE;
                	default:
                        throw new UnsupportedOperationException("not supported Logic type in flatbuffers: " + logicType);
                	}

            default:
                throw new UnsupportedOperationException("not supported column type in flatbuffers: " + columnType);
        }
    }

    @Override
    public SQLTableMeta getSQLTableMeta() {
        return sqlTableMeta;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public long columnCount() {
        return columnCount;
    }

    @Override
    public Object get(int rowIndex, int columnIndex) {
        if (rowIndex >= rowCount() || rowIndex < 0) {
            throw new IllegalStateException("Row index " + columnIndex + " out of range");
        }
        if (columnIndex >= columnCount || columnIndex < 0) {
            throw new IllegalStateException("Column index " + columnIndex + " out of range");
        }
        byte columnType = columnTypes[columnIndex];
        byte logicType = columnComplexTypeInfos[columnIndex].columnLogicType();
        byte encodingType = columnComplexTypeInfos[columnIndex].columnEncodeType();
        ColumnValues columnValue = columnValues[columnIndex];
        switch (columnType) {
            case DataType.LONG:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    return columnValue.longValues(rowIndex);
                }
            case DataType.BOOLEAN:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    return columnValue.boolValues(rowIndex);
                }
            case DataType.DOUBLE:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    return columnValue.doubleValues(rowIndex);
                }
            case DataType.STRING:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    return columnValue.stringValues(rowIndex);
                }
            case DataType.BINARY:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    BytesValue bytesValue = columnValue.binaryValues(rowIndex);
                    return bytesValue.valueAsByteBuffer().duplicate();
                }
            case DataType.STRING_RLE:
                if (columnValue.isNullvalues(rowIndex)) {
                    return null;
                } else {
                    RLEStringValues rleStringValue = rleStringValues[columnIndex];
                    return resolveRLEString(rleStringValue, rowIndex);
                }
            case DataType.COMPLEX:
                switch (logicType) {
                    case LogicType.DATETIME:
                        if(encodingType != DataType.LONG){
                            throw new UnsupportedOperationException("encoding Type need to be: " + DataType.LONG +" , but get: " + encodingType);
                        }
                        return resolveDateTime(columnValue.longValues(rowIndex));
                    case LogicType.TIME:
                        if(encodingType != DataType.LONG){
                            throw new UnsupportedOperationException("encoding Type need to be: " + DataType.LONG +" , but get: " + encodingType);
                        }
                        return resolveTime(columnValue.longValues(rowIndex));
                    case LogicType.DATE:
                        if(encodingType != DataType.LONG){
                            throw new UnsupportedOperationException("encoding Type need to be: " + DataType.LONG +" , but get: " + encodingType);
                        }
                        return resolveDate(columnValue.longValues(rowIndex));
                    default:
                        throw new UnsupportedOperationException("not supported Logic type in flatbuffers: " + logicType);
                }
            default:
                throw new UnsupportedOperationException("not supported column type in flatbuffers: " + columnType);
        }
    }

    private String resolveRLEString(RLEStringValues rleStringValue, int rowIndex) {
        return rleStringValue.array(rleStringValue.indexMapping(rowIndex));
    }

    private ZonedDateTime resolveDateTime(long ts) {
        return Instant.ofEpochSecond(ts / 1000000, (int) (ts % 1000000) * 1000).atZone(ZoneOffset.UTC);
    }

    private Duration resolveTime(long nanos) {
        return Duration.ofNanos(nanos);
    }

    private LocalDate resolveDate(long ts) {
        return Instant.ofEpochSecond(ts)
                .atZone(ZoneOffset.UTC)
                .toLocalDate();
    }

}
