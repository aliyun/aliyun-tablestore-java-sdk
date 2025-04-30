package com.alicloud.openservices.tablestore.model;

import java.io.IOException;
import java.util.Arrays;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Base64;
import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.*;

/**
 * Represents the value of an attribute column.
 */
public class ColumnValue implements Comparable<ColumnValue>, Jsonizable, Measurable {

    /**
     * For internal use only. Do not use it.
     */
    public static ColumnValue INTERNAL_NULL_VALUE = new ColumnValue(null, ColumnType.STRING);

    private Object value;
    private ColumnType type;
    private int dataSize = -1;

    public Object getValue() {
        return value;
    }

    public ColumnValue(Object value, ColumnType type) {
        this.value = value;
        this.type = type;
    }
    
    private int calculateDataSize() {
        int dataSize = 0;
        switch (this.type) {
        case INTEGER:
            dataSize = 8;
            break;
        case STRING:
        	if (value == null) {
        		dataSize = 0;
        	} else {
        		dataSize = CalculateHelper.calcStringSizeInBytes(this.asString());
        	}
            break;
        case BINARY:
            dataSize = this.asBinary().length;
            break;
        case DOUBLE:
            dataSize = 8;
            break;
        case BOOLEAN:
            dataSize = 1;
            break;
        default:
            throw new IllegalStateException("Bug: not support the type : " + type);
        }
        return dataSize;
    }
    
    /**
     * Get the size of the primary key column value, the size calculation formula for each type is as follows:
     *  - {@link ColumnType#INTEGER}: Constant size of 8 bytes
     *  - {@link ColumnType#DOUBLE}: Constant size of 8 bytes
     *  - {@link ColumnType#BOOLEAN}: Constant size of 1 byte
     *  - {@link ColumnType#BINARY}: Size equals the number of bytes
     *  - {@link ColumnType#STRING}: Size equals the number of bytes after UTF-8 encoding
     *
     * @return The size of the value
     */
    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            dataSize = calculateDataSize();
        }
        return this.dataSize;
    }

    /**
     * Get the type of the attribute column.
     *
     * @return The type of the attribute column.
     */
    public ColumnType getType() {
        return type;
    }

    /**
     * Construct an attribute column of type {@link ColumnType#STRING}.
     * <p>Note: The value must not be a null pointer.</p>
     *
     * @param value A value of string type.
     * @return The generated instance.
     */
    public static ColumnValue fromString(String value) {
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        return new ColumnValue(value, ColumnType.STRING);
    }

    /**
     * Constructs a property column of type {@link ColumnType#INTEGER}.
     *
     * @param value The long integer value.
     * @return The generated instance
     */
    public static ColumnValue fromLong(long value) {
        return new ColumnValue(value, ColumnType.INTEGER);
    }

    /**
     * Constructs a property column of type {@link ColumnType#BINARY}.
     * <p>Note: The value cannot be a null pointer.</p>
     *
     * @param value The value of binary string type.
     * @return The generated instance
     */
    public static ColumnValue fromBinary(byte[] value) {
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        return new ColumnValue(value, ColumnType.BINARY);
    }

    /**
     * Constructs a property column of type {@link ColumnType#DOUBLE}.
     *
     * @param value A value of type double.
     * @return The generated instance.
     */
    public static ColumnValue fromDouble(double value) {
        return new ColumnValue(value, ColumnType.DOUBLE);
    }

    /**
     * Constructs a property column of type {@link ColumnType#BOOLEAN}.
     *
     * @param value A boolean type value.
     * @return The generated instance
     */
    public static ColumnValue fromBoolean(boolean value) {
        return new ColumnValue(value, ColumnType.BOOLEAN);
    }

    /**
     * Get the string value of the attribute column.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#STRING}.</p>
     *
     * @return String value
     */
    public String asString() {
        if (this.type != ColumnType.STRING) {
            throw new IllegalStateException("The type of column is not STRING.");
        }

        return (String) value;
    }

    public byte[] asStringInBytes() {
        return Bytes.toBytes(asString());
    }

    /**
     * Get the long integer value of the attribute column.
     * <p>Currently, the value can only be retrieved when the data type is {@link ColumnType#INTEGER}.</p>
     *
     * @return Long integer value
     */
    public long asLong() {
        if (this.type != ColumnType.INTEGER) {
            throw new IllegalStateException("The type of column is not INTEGER.");
        }

        return (Long) value;
    }

    /**
     * Get the value of the attribute column as a binary string.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BINARY}.</p>
     *
     * @return The value as a binary string
     */
    public byte[] asBinary() {
        if (this.type != ColumnType.BINARY) {
            throw new IllegalStateException("The type of column is not BINARY.");
        }
        return (byte[]) value;
    }

    /**
     * Get the value of the attribute column with DOUBLE type.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#DOUBLE}.</p>
     *
     * @return Value in Double type
     */
    public double asDouble() {
        if (this.type != ColumnType.DOUBLE) {
            throw new IllegalStateException("The type of column is not DOUBLE.");
        }
        return (Double) value;
    }

    /**
     * Get the boolean value of the attribute column.
     * <p>Currently, the value can only be obtained when the data type is {@link ColumnType#BOOLEAN}.</p>
     *
     * @return The boolean value
     */
    public boolean asBoolean() {
        if (this.type != ColumnType.BOOLEAN) {
            throw new IllegalStateException("The type of column is not BOOLEAN.");
        }
        return (Boolean) value;
    }

    /**
     * Get a checksum using the crc8 algorithm, mainly used to calculate the checksum of a cell
     * @param crc initial value of crc
     * @return
     */
    public byte getChecksum(byte crc) throws IOException {
        switch (getType()) {
            case STRING: {
                byte[] rawData = asStringInBytes();
                crc = PlainBufferCrc8.crc8(crc, VT_STRING);
                crc = PlainBufferCrc8.crc8(crc, rawData.length);
                crc = PlainBufferCrc8.crc8(crc, rawData);
                break;
            }
            case INTEGER: {
                crc = PlainBufferCrc8.crc8(crc, VT_INTEGER);
                crc = PlainBufferCrc8.crc8(crc, asLong());
                break;
            }
            case BINARY: {
                byte[] rawData = asBinary();
                crc = PlainBufferCrc8.crc8(crc, VT_BLOB);
                crc = PlainBufferCrc8.crc8(crc, rawData.length);
                crc = PlainBufferCrc8.crc8(crc, rawData);
                break;
            }
            case DOUBLE: {
                crc = PlainBufferCrc8.crc8(crc, VT_DOUBLE);
                crc = PlainBufferCrc8.crc8(crc, Double.doubleToRawLongBits(asDouble()));
                break;
            }
            case BOOLEAN: {
                crc = PlainBufferCrc8.crc8(crc, VT_BOOLEAN);
                crc = PlainBufferCrc8.crc8(crc, asBoolean() ? (byte) 0x1 : (byte) 0x0);
                break;
            }
            default:
                throw new IOException("Bug: unsupported column type: " + getType());
        }
        return crc;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ColumnValue)) {
            return false;
        }

        ColumnValue val = (ColumnValue) o;
        if (this.type == val.type) {
            if (this.type == ColumnType.BINARY) {
                return Bytes.equals((byte[]) value, (byte[]) val.value);
            } else {
                if (this.value == null) {
                    return val.value == null;
                } else {
                    return this.value.equals(val.value);
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (this.type == ColumnType.BINARY) {
            return Arrays.hashCode(asBinary()) * 31 + this.type.hashCode();
        } else {
            return this.value.hashCode() * 31 + this.type.hashCode();
        }
    }

    @Override
    public String toString() {
        if (this.value == null) {
            return "null";
        }
        if (this.type == ColumnType.BINARY) {
            return Arrays.toString(asBinary());
        } else {
            return this.value.toString();
        }
    }

    /**
     * Compare the values of two property columns.
     * <p>Note: Property columns of different types cannot be compared.</p>
     *
     * @param target
     * @return Returns a value greater than 0 if it is greater, 0 if equal, and less than 0 if it is smaller
     */
    @Override
    public int compareTo(ColumnValue target) {
        if (this.type != target.type) {
            throw new IllegalArgumentException("The type of column to compare must be the same.");
        }

        switch (this.type) {
            case STRING:
                return ((String) value).compareTo(target.asString());
            case INTEGER:
                return ((Long) value).compareTo(target.asLong());
            case BINARY:
                byte[] b1 = (byte[]) this.value;
                byte[] b2 = (byte[]) target.value;
                int ret = Bytes.compareByteArrayInLexOrder(b1, 0, b1.length, b2, 0, b2.length);
                return ret;
            case DOUBLE:
                return ((Double) value).compareTo(target.asDouble());
            case BOOLEAN:
                return ((Boolean) value).compareTo(target.asBoolean());
            default:
                throw new IllegalArgumentException("Unknown type: " + this.type);
        }
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Type\": \"");
        sb.append(type.toString());
        sb.append("\", \"Value\": ");

        switch(type) {
        case INTEGER:
            sb.append(asLong());
            break;
        case BINARY:
            sb.append("\"");
            sb.append(Base64.toBase64String(asBinary()));
            sb.append("\"");
            break;
        case DOUBLE:
            sb.append(asDouble());
            break;
        case BOOLEAN:
            sb.append(asBoolean() ? "true" : "false");
            break;
        case STRING:
            sb.append("\"");
            sb.append(asString());
            sb.append("\"");
            break;
        default:
            throw new IllegalArgumentException("Unknown type: " + type);
        }
        
        sb.append("}");
    }
}

