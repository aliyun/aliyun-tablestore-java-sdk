package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Data point, includes:
 * 1. {@link TimestreamIdentifier}: Timeline identifier
 * 2. timestamp: The timestamp of the data point
 * 3. fields: The list of data values
 */
public class Point {
    public static final class Builder {
        private long timestamp;
        private List<Column> fieldList;

        public Builder(long timestamp, TimeUnit unit) {
            if (timestamp < 0) {
                throw new ClientException("The timestamp must be positive.");
            }
            this.timestamp = unit.toMicros(timestamp);
            this.fieldList = new ArrayList<Column>();
        }

        public Builder addField(String key, float value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromDouble(value)));
            return this;
        }

        public Builder addField(String key, double value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromDouble(value)));
            return this;
        }

        public Builder addField(String key, int value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromLong(value)));
            return this;
        }

        public Builder addField(String key, long value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromLong(value)));
            return this;
        }

        public Builder addField(String key, String value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromString(value)));
            return this;
        }

        public Builder addField(String key, boolean value) {
            this.fieldList.add(
                    new Column(
                            key,
                            ColumnValue.fromBoolean(value)));
            return this;
        }

        private Builder addField(String key, Object value) {
            if (value instanceof String) {
                addField(key, (String)value);
            } else if (value instanceof Double) {
                addField(key, ((Double) value).doubleValue());
            } else if (value instanceof Float) {
                addField(key, ((Float) value).floatValue());
            } else if (value instanceof Long) {
                addField(key, ((Long) value).longValue());
            } else if (value instanceof Integer) {
                addField(key, ((Integer) value).intValue());
            } else if (value instanceof Boolean) {
                addField(key, ((Boolean) value).booleanValue());
            } else {
                throw new ClientException("Unsupported type, must be String/Double/Float/Long/Integer/Boolean");
            }
            return this;
        }

        /**
         * Serialize the public fields of a class object as 'filed'. 
         * The class fields only support String, Double, Float, Long, Integer, Boolean.
         */
        public Builder from(Object object){
            try {
                Class c = object.getClass();
                for (Field field : c.getFields()) {
                    com.alicloud.openservices.tablestore.timestream.model.annotation.Field colAnnotation =
                            field.getAnnotation(com.alicloud.openservices.tablestore.timestream.model.annotation.Field.class);
                    if (colAnnotation != null) {
                        if (!field.isAccessible()) {
                            field.setAccessible(true);
                        }
                        addField(colAnnotation.name(), field.get(object));
                    }
                }
            } catch(IllegalAccessException e) {
                throw new ClientException(
                        e.toString()
                );
            }
            return this;
        }

        public Point build() {
            Point point = new Point(timestamp, TimeUnit.MICROSECONDS);
            point.setFields(fieldList);
            return point;
        }

    }
    private static Logger logger = LoggerFactory.getLogger(Point.class);
    /**
     * The timestamp of the data point, in units of us
     */
    private long timestamp;
    private List<Column> fieldList;

    protected Point(Row row) {
        this.timestamp = row.getPrimaryKey().getPrimaryKeyColumn(TableMetaGenerator.CN_TAMESTAMP_NAME).getValue().asLong();
        this.fieldList = new ArrayList<Column>();
        addField(row);
    }

    private Point(long timestamp, TimeUnit unit) {
        this.timestamp = unit.toMicros(timestamp);
        this.fieldList = new ArrayList<Column>();
    }

    protected Point(long timestamp, TimeUnit unit, Row row) {
        this.timestamp = unit.toMicros(timestamp);
        this.fieldList = new ArrayList<Column>();
        addField(row);
    }

    /**
     * @return The timestamp of the data point, in units of us
     */
    public long getTimestamp() {
        return getTimestamp(TimeUnit.MICROSECONDS);
    }

    /**
     *
     * @param unit The unit of the returned timestamp
     * @return The timestamp of the data point
     */
    public long getTimestamp(TimeUnit unit) {
        return unit.convert(this.timestamp, TimeUnit.MICROSECONDS);
    }

    protected Point addField(Column col) {
        this.fieldList.add(col);
        return this;
    }

    private Point setFields(List<Column> cols) {
        this.fieldList = cols;
        return this;
    }

    public List<Column> getFields() {
        return this.fieldList;
    }

    public ColumnValue getField(String name) {
        ColumnValue value = null;
        for (Column col : this.fieldList) {
            if (col.getName().equals(name)) {
                value = col.getValue();
            }
        }
        return value;
    }

    private void addField(Row row) {
        for (Map.Entry<String, NavigableMap<Long, ColumnValue>> e : row.getColumnsMap().entrySet()) {
            this.fieldList.add(row.getLatestColumn(e.getKey()));
        }
    }

    public String toString() {
        Map<String, Object> fields = new HashMap<String, Object>();
        for (Column col : this.fieldList) {
            fields.put(col.getName(), col.getValue().toString());
        }
        StringBuilder sb = new StringBuilder();
        sb.append("timestamp=").append(timestamp).append(", ");
        sb.append("fileds=").append(fields);
        return sb.toString();
    }
}
