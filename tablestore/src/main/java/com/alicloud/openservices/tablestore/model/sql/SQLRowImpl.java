package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.model.ColumnType;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;

public class SQLRowImpl implements SQLRow {

    private SQLRows sqlRows;

    private int rowIndex;

    public SQLRowImpl(SQLRows sqlRows, int rowIndex) {
        this.sqlRows = sqlRows;
        this.rowIndex = rowIndex;
    }

    @Override
    public Object get(int columnIndex) {
        checkValid(columnIndex, null);
        return sqlRows.get(rowIndex, columnIndex);
    }

    @Override
    public Object get(String name) {
        checkValid(name, null);
        return get(sqlRows.getSQLTableMeta().getColumnsMap().get(name));
    }

    @Override
    public String getString(int columnIndex) {
        checkValid(columnIndex, ColumnType.STRING);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (String) value;
        }
    }

    @Override
    public String getString(String name) {
        checkValid(name, ColumnType.STRING);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (String) value;
        }
    }

    @Override
    public Long getLong(int columnIndex) {
        checkValid(columnIndex, ColumnType.INTEGER);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (Long) value;
        }
    }

    @Override
    public Long getLong(String name) {
        checkValid(name, ColumnType.INTEGER);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (Long) value;
        }
    }

    @Override
    public Boolean getBoolean(int columnIndex) {
        checkValid(columnIndex, ColumnType.BOOLEAN);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (Boolean) value;
        }
    }

    @Override
    public Boolean getBoolean(String name) {
        checkValid(name, ColumnType.BOOLEAN);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (Boolean) value;
        }
    }

    @Override
    public Double getDouble(int columnIndex) {
        checkValid(columnIndex, ColumnType.DOUBLE);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (Double) value;
        }
    }

    @Override
    public Double getDouble(String name) {
        checkValid(name, ColumnType.DOUBLE);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (Double) value;
        }
    }

    @Override
    public ZonedDateTime getDateTime(int columnIndex) {
        checkValid(columnIndex, ColumnType.DATETIME);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (ZonedDateTime) value;
        }
    }

    @Override
    public ZonedDateTime getDateTime(String name) {
        checkValid(name, ColumnType.DATETIME);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (ZonedDateTime) value;
        }
    }

    @Override
    public Duration getTime(int columnIndex) {
        checkValid(columnIndex, ColumnType.TIME);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (Duration) value;
        }
    }

    @Override
    public Duration getTime(String name) {
        checkValid(name, ColumnType.TIME);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (Duration) value;
        }
    }

    @Override
    public LocalDate getDate(int columnIndex) {
        checkValid(columnIndex, ColumnType.DATE);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (LocalDate) value;
        }
    }

    @Override
    public LocalDate getDate(String name) {
        checkValid(name, ColumnType.DATE);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (LocalDate) value;
        }
    }

    @Override
    public ByteBuffer getBinary(int columnIndex) {
        checkValid(columnIndex, ColumnType.BINARY);
        Object value = get(columnIndex);
        if (value == null) {
            return null;
        } else {
            return (ByteBuffer) value;
        }
    }

    @Override
    public ByteBuffer getBinary(String name) {
        checkValid(name, ColumnType.BINARY);
        Object value = get(name);
        if (value == null) {
            return null;
        } else {
            return (ByteBuffer) value;
        }
    }

    @Override
    public String toDebugString() {
        StringBuilder sb = new StringBuilder();
        List<SQLColumnSchema> schemas = sqlRows.getSQLTableMeta().getSchema();
        for (int i = 0; i < schemas.size(); i++) {
            sb.append(schemas.get(i).getName() + ": ");
            sb.append(get(i));
            if (i < schemas.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private void checkValid(int columnIndex, ColumnType responseType) {
        if (columnIndex >= sqlRows.columnCount() || columnIndex < 0) {
            throw new UnsupportedOperationException("Column index " + columnIndex + " is out of range");
        }
        if (responseType != null && responseType != sqlRows.getSQLTableMeta().getSchema().get(columnIndex).getType()) {
            throw new UnsupportedOperationException("Column type collates failed, response type: " + responseType +
                    ", but the real is: " + sqlRows.getSQLTableMeta().getSchema().get(columnIndex).getType());
        }
    }

    private void checkValid(String name, ColumnType responseType) {
        if (!sqlRows.getSQLTableMeta().getColumnsMap().containsKey(name)) {
            throw new IllegalStateException("SQLRow doesn't contains field name: " + name);
        }
        int columnIndex = sqlRows.getSQLTableMeta().getColumnsMap().get(name);
        if (responseType != null && responseType != sqlRows.getSQLTableMeta().getSchema().get(columnIndex).getType()) {
            throw new UnsupportedOperationException("Column type collates failed, response type: " + responseType +
                    ", but the real is: " + sqlRows.getSQLTableMeta().getSchema().get(columnIndex).getType());
        }
    }

}
