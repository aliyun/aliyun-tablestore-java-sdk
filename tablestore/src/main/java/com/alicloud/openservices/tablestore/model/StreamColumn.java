package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class StreamColumn {
    public enum Type {
        INVALID,
        SPECIFIED_COLUMN,
        INPUT_COLUMNS,
        ALL_COLUMNS;

        public StreamColumnType toStreamColumnType() {
            return StreamColumnType.valueOf(name());
        }

        public static Type fromStreamColumnType(StreamColumnType type) {
            if (type == null) {
                return null;
            }
            return Type.valueOf(type.name());
        }
    }

    private StreamColumnType columnType;
    private final List<String> columnNames = new ArrayList<String>();

    public StreamColumn(StreamColumnType columnType) {
        setColumnType(columnType);
    }

    public StreamColumn(StreamColumnType columnType, Collection<String> columnNames) {
        setColumnType(columnType);
        addColumnNamesIfPresent(columnNames);
    }

    public StreamColumn(Type columnType) {
        setType(columnType);
    }

    public StreamColumn(Type columnType, Collection<String> columnNames) {
        setType(columnType);
        addColumnNamesIfPresent(columnNames);
    }

    public StreamColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(StreamColumnType columnType) {
        Preconditions.checkNotNull(columnType, "columnType should not be null.");
        this.columnType = columnType;
    }

    public Type getType() {
        return Type.fromStreamColumnType(columnType);
    }

    public void setType(Type type) {
        Preconditions.checkNotNull(type, "type should not be null.");
        this.columnType = type.toStreamColumnType();
    }

    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    public void addColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(),
            "columnName should not be null or empty.");
        this.columnNames.add(columnName);
    }

    public void addColumnNames(Collection<String> columnNames) {
        Preconditions.checkNotNull(columnNames, "columnNames should not be null.");
        for (String columnName : columnNames) {
            addColumnName(columnName);
        }
    }

    private void addColumnNamesIfPresent(Collection<String> columnNames) {
        if (columnNames != null) {
            addColumnNames(columnNames);
        }
    }

    @Override
    public String toString() {
        return "StreamColumn{" +
            "columnType=" + columnType +
            ", columnNames=" + columnNames +
            '}';
    }
}
