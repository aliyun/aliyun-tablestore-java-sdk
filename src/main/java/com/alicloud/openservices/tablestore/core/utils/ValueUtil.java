package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.model.ColumnValue;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;

public class ValueUtil {

    public static ColumnValue toColumnValue(Object value) {
        if (value instanceof Long) {
            return ColumnValue.fromLong((Long)value);
        } else if (value instanceof Integer) {
            return ColumnValue.fromLong(((Integer)value).longValue());
        } else if (value instanceof Double) {
            return ColumnValue.fromDouble((Double)value);
        } else if (value instanceof String) {
            return ColumnValue.fromString((String)value);
        } else if (value instanceof Boolean) {
            return ColumnValue.fromBoolean((Boolean)value);
        } else if (value instanceof byte[]) {
            return ColumnValue.fromBinary((byte[])value);
        } else {
            throw new IllegalArgumentException("unsupported type: " + value.getClass());
        }
    }

    public static Object toObject(ColumnValue value) {
        switch (value.getType()) {
            case INTEGER: {
                return value.asLong();
            }
            case STRING: {
                return value.asString();
            }
            case BOOLEAN: {
                return value.asBoolean();
            }
            case DOUBLE: {
                return value.asDouble();
            }
            case BINARY: {
                return value.asBinary();
            }
            default: {
                throw new RuntimeException("unexpected");
            }
        }
    }

    public static ZonedDateTime parseMicroTimestampToUTCDateTime(long ts){
        return Instant.ofEpochSecond(ts / 1000000, (int) (ts % 1000000) * 1000).atZone(ZoneOffset.UTC);
    }

    public static long parseDateTimeToMicroTimestamp(ZonedDateTime zdt){
        if (zdt.getNano() % 1000 != 0){
            throw new RuntimeException("datetime precision exceed, please ensure the precision is microsecond");
        }
        return zdt.toEpochSecond() * 1000000 + zdt.getNano() / 1000;
    }
}
