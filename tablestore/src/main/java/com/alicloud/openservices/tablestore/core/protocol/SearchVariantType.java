package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SearchVariantType {

    enum VariantType {
        INTEGER,
        DOUBLE,
        BOOLEAN,
        STRING
    }

    // variant type
    public final static byte VT_INTEGER = 0x0;
    public final static byte VT_DOUBLE = 0x1;
    public final static byte VT_BOOLEAN = 0x2;
    public final static byte VT_STRING = 0x3;

    public static VariantType GetVariantType(byte[] data) {
        if (data[0] == VT_INTEGER) {
            return VariantType.INTEGER;
        } else if (data[0] == VT_DOUBLE) {
            return VariantType.DOUBLE;
        } else if (data[0] == VT_BOOLEAN) {
            return VariantType.BOOLEAN;
        } else if (data[0] == VT_STRING) {
            return VariantType.STRING;
        } else {
            throw new IllegalArgumentException("unknown type: " + data[0]);
        }
    }

    public static long asLong(byte[] data) {
        return ByteBuffer.wrap(data, 1, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    public static byte[] fromLong(long v) {
        ByteBuffer buffer = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        return buffer.put(VT_INTEGER).putLong(v).array();
    }

    public static double asDouble(byte[] data) {
        return ByteBuffer.wrap(data, 1, 8).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    }

    public static byte[] fromDouble(double v) {
        ByteBuffer buffer = ByteBuffer.allocate(9).order(ByteOrder.LITTLE_ENDIAN);
        return buffer.put(VT_DOUBLE).putDouble(v).array();
    }

    public static String asString(byte[] data) {
        int length = ByteBuffer.wrap(data, 1, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return new String(data, 5, length, Constants.UTF8_CHARSET);
    }

    public static byte[] fromString(String v) {
        byte[] vBytes = v.getBytes(Constants.UTF8_CHARSET);
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + vBytes.length).order(ByteOrder.LITTLE_ENDIAN);
        return buffer.put(VT_STRING).putInt(vBytes.length).put(vBytes).array();
    }

    public static boolean asBoolean(byte[] data) {
        return data[1] != 0;
    }

    public static byte[] fromBoolean(boolean v) {
        return new byte[]{VT_BOOLEAN, (byte) (v?1:0)};
    }

    public static Object getValue(byte[] data) {
        if (data[0] == VT_INTEGER) {
            return asLong(data);
        } else if (data[0] == VT_DOUBLE) {
            return asDouble(data);
        } else if (data[0] == VT_STRING) {
            return asString(data);
        } else if (data[0] == VT_BOOLEAN) {
            return asBoolean(data);
        } else {
            throw new IllegalArgumentException("unknown type: " + data[0]);
        }
    }

    public static byte[] toVariant(ColumnValue value) {
        switch (value.getType()) {
            case STRING:
                return fromString(value.asString());
            case INTEGER:
                return fromLong(value.asLong());
            case DOUBLE:
                return fromDouble(value.asDouble());
            case BOOLEAN:
                return fromBoolean(value.asBoolean());
            default:
                throw  new IllegalArgumentException("unsupported type:" + value.getType().name());
        }
    }

    public static ColumnValue forceConvertToDestColumnValue(byte[] data) throws IOException {

        if (data.length == 0) {
            throw new IOException("data is null");
        }
        ColumnValue columnValue = null;
        if (data[0] == VT_INTEGER) {
            columnValue = new ColumnValue(asLong(data), ColumnType.INTEGER);
        } else if (data[0] == VT_DOUBLE) {
            columnValue = new ColumnValue(asDouble(data), ColumnType.DOUBLE);
        } else if (data[0] == VT_STRING) {
            columnValue = new ColumnValue(asString(data), ColumnType.STRING);
        } else if (data[0] == VT_BOOLEAN) {
            columnValue = new ColumnValue(asBoolean(data), ColumnType.BOOLEAN);
        } else {
            throw new IOException("Bug: unsupported data type");
        }

        return columnValue;
    }
}
