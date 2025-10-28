package com.alicloud.openservices.tablestore.model;

import java.io.IOException;
import java.util.Arrays;

import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_BLOB;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_INF_MIN;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_INF_MAX;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_STRING;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_INTEGER;
import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.VT_AUTO_INCREMENT;

/**
 * Represents the value of a primary key column.
 * <p>To construct a primary key column of type {@link PrimaryKeyType#INTEGER}, use {@link #fromLong(long)} to initialize it.</p>
 * <p>To construct a primary key column of type {@link PrimaryKeyType#STRING}, use {@link #fromString(String)} to initialize it.</p>
 * <p>To construct a primary key column of type {@link PrimaryKeyType#BINARY}, use {@link #fromBinary(byte[])} to initialize it.</p>
 * <p>Note: {@link #INF_MIN} and {@link #INF_MAX} are special primary key columns that are only used to indicate the range of primary key columns in the 
 * {@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)} operation. They cannot be used as actual data written to TableStore, 
 * nor can they be used as parameters for any read operations other than GetRange.</p>
 */
public class PrimaryKeyValue implements Comparable<PrimaryKeyValue>, Measurable {

    /**
     * Represents the maximum value of the primary key range, which is only used for the 
     * {@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)}
     * operation to indicate the range of the primary key column. It cannot be used as actual data written to TableStore, 
     * nor can it be used as a parameter for any read operations other than GetRange.
     */
    public static final PrimaryKeyValue INF_MAX = new PrimaryKeyValue("INF_MAX", null);
    /**
     * Represents the minimum value of the primary key range, which is only used for the 
     * {@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)}
     * operation to indicate the range of the primary key column. It cannot be used as actual data to be written into TableStore, 
     * nor can it be used as a parameter for any read operations other than GetRange.
     */
    public static final PrimaryKeyValue INF_MIN = new PrimaryKeyValue("INF_MIN", null);
    
    /**
     * Indicates that the primary key value is retained, and its only purpose is for filling in PK increment columns.
     * When a PK column is set as an increment column, during PutRow or UpdateRow operations, if the user specifies the value of this column as AUTO_INCREMENT, OTS will automatically assign a larger value to this column after the row is successfully written, ensuring that the value of this column is permanently increasing.
     */
    @Deprecated
    public static final PrimaryKeyValue AUTO_INCRMENT = new PrimaryKeyValue("AUTO_INCREMENT", null);

    public static final PrimaryKeyValue AUTO_INCREMENT = new PrimaryKeyValue("AUTO_INCREMENT", null);

    private Object value;
    private byte[] rawData; // raw bytes for utf-8 string
    private PrimaryKeyType type;
    private int dataSize = 0;

    private PrimaryKeyValue(Object value, PrimaryKeyType type) {
        this.value = value;
        this.type = type;

        if (this.type != null) {
            switch (this.type) {
                case INTEGER:
                    this.dataSize = 8;
                    break;
                case STRING:
                    this.dataSize = CalculateHelper.calcStringSizeInBytes(this.asString());
                    break;
                case BINARY:
                    this.dataSize = this.asBinary().length;
                    break;
                default:
                    throw new IllegalStateException("Bug: not support the type : " + type);
            }
        }
    }

    /**
     * Get the type of the primary key column.
     *
     * @return The type of the primary key column.
     */
    public PrimaryKeyType getType() {
        return type;
    }

    @Deprecated
    public int getSize() {
        return getDataSize();
    }

    /**
     * Get the size of the primary key column value, the size calculation formula for each type is:
     *  - {@link PrimaryKeyType#INTEGER}: Constant size of 8 bytes
     *  - {@link PrimaryKeyType#STRING}: Size is the number of bytes after UTF-8 encoding
     *
     * @return The size of the value
     */
    @Override
    public int getDataSize() {
        return this.dataSize;
    }

    /**
     * Constructs a primary key column of type {@link PrimaryKeyType#STRING}.
     * <p>Note: The value cannot be a null pointer.</p>
     *
     * @param value The value of string type.
     * @return The generated object
     */
    public static PrimaryKeyValue fromString(String value) {
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");
        return new PrimaryKeyValue(value, PrimaryKeyType.STRING);
    }

    /**
     * Constructs a primary key column of type {@link PrimaryKeyType#STRING}.
     * <p>Note: The value cannot be a null pointer.</p>
     *
     * @param value The value of string type.
     * @param rawData The raw bytes for utf-8 string.
     * @return The generated object
     */
    public static PrimaryKeyValue fromString(String value, byte[] rawData) {
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");
        Preconditions.checkNotNull(rawData, "The value of rawData should not be null.");
        PrimaryKeyValue primaryKeyValue = new PrimaryKeyValue(value, PrimaryKeyType.STRING);
        primaryKeyValue.rawData = rawData;
        return primaryKeyValue;
    }

    /**
     * Constructs a primary key column of type {@link PrimaryKeyType#INTEGER}.
     *
     * @param value The long integer value.
     * @return The generated object
     */
    public static PrimaryKeyValue fromLong(long value) {
        return new PrimaryKeyValue(value, PrimaryKeyType.INTEGER);
    }

    /**
     * Construct a primary key column of type {@link PrimaryKeyType#BINARY}.
     * <p>Note: The value cannot be a null pointer.</p>
     *
     * @param value The value of binary string type.
     * @return The generated object
     */
    public static PrimaryKeyValue fromBinary(byte[] value) {
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");
        return new PrimaryKeyValue(value, PrimaryKeyType.BINARY);
    }

    public static PrimaryKeyValue fromColumn(ColumnValue value) {
        switch (value.getType()) {
            case STRING:
                return fromString(value.asString());
            case INTEGER:
                return fromLong(value.asLong());
            case BINARY:
                return fromBinary(value.asBinary());
            default:
                throw new IllegalArgumentException("Can not convert from column with not compatible type: " + value.getType());
        }
    }

    /**
     * Get a checksum using the crc8 algorithm, mainly used to calculate the checksum of a cell
     * @param crc
     * @return checksum
     */
    public byte getChecksum(byte crc) throws IOException {
        if (isInfMin()) {
            crc = PlainBufferCrc8.crc8(crc, VT_INF_MIN);
            return crc;
        }
        if (isInfMax()) {
            crc = PlainBufferCrc8.crc8(crc, VT_INF_MAX);
            return crc;
        }
        if (isPlaceHolderForAutoIncr()) {
            crc = PlainBufferCrc8.crc8(crc, VT_AUTO_INCREMENT);
            return crc;
        }

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
            default:
                throw new IOException("Bug: unsupported column type: " + getType());
        }
        return crc;
    }

    public ColumnValue toColumnValue() throws IOException {
        ColumnValue value = null;
        if (isInfMin() || isInfMax() || isPlaceHolderForAutoIncr()) {
            throw new IOException("Can not convert INF_MIN or INF_MAX or AUTO_INCREMENT into column value.");
        }
        switch (getType()) {
            case INTEGER: {
                value = ColumnValue.fromLong(asLong());
                break;
            }
            case STRING: {
                value = ColumnValue.fromString(asString());
                break;
            }
            case BINARY: {
                value = ColumnValue.fromBinary(asBinary());
                break;
            }
            default:
                throw new IOException("Unsupported primary key type: " + getType());
        }
        return value;
    }

    /**
     * Get the string value of the primary key column.
     * <p>Currently, the value can only be obtained when the data type is {@link PrimaryKeyType#STRING}.</p>
     *
     * @return The string value
     */
    public String asString() {
        if (this.type != PrimaryKeyType.STRING) {
            throw new IllegalStateException("The type of primary key is not STRING.");
        }

        return (String) value;
    }

    public byte[] asStringInBytes() {
        if (rawData == null) {
            rawData = Bytes.toBytes(asString());
        }
        return rawData;
    }

    /**
     * Get the long integer value of the primary key column.
     * <p>Currently, the value can only be obtained when the data type is {@link PrimaryKeyType#INTEGER}.</p>
     *
     * @return Long integer value
     */
    public long asLong() {
        if (this.type != PrimaryKeyType.INTEGER) {
            throw new IllegalStateException("The type of primary key is not INTEGER.");
        }

        return (Long) value;
    }

    /**
     * Get the value of the primary key column in binary string type.
     * <p>Currently, the value can only be obtained when the data type is {@link PrimaryKeyType#BINARY}.</p>
     *
     * @return The value in binary string type
     */
    public byte[] asBinary() {
        if (this.type != PrimaryKeyType.BINARY) {
            throw new IllegalStateException("The type of primary key is not BINARY");
        }
        return (byte[]) value;
    }

    /**
     * For internal use. DO NOT USE.
     * @return true if it is INF_MIN
     */
    public boolean isInfMin() {
        return type == null && value.equals("INF_MIN");
    }

    /**
     * For internal use. DO NOT USE.
     * @return true if it is INF_MAX
     */
    public boolean isInfMax() {
        return type == null && value.equals("INF_MAX");
    }
  
    /**
     * For internal use. DO NOT USE.
     * @return true if it is AUTO_INCREMENT
     */
    public boolean isPlaceHolderForAutoIncr() {
        return type == null && value.equals("AUTO_INCREMENT");
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKeyValue)) {
            return false;
        }

        PrimaryKeyValue val = (PrimaryKeyValue) o;
        if (this.type == val.type) {
            if (this.type == PrimaryKeyType.BINARY) {
                return Bytes.equals((byte[]) value, (byte[]) val.value);
            } else {
                return this.value.equals(val.value);
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        if (this.type == PrimaryKeyType.BINARY) {
            return Arrays.hashCode(asBinary()) * 31 + this.type.hashCode();
        } else {
            return this.value.hashCode() * 31 + (this.type != null ? this.type.hashCode() : 0 );
        }
    }

    @Override
    public String toString() {
        if (this.type == PrimaryKeyType.BINARY) {
            return Arrays.toString(asBinary());
        } else {
            return this.value.toString();
        }
    }

    /**
     * Compare the values of two primary key columns.
     * <p>Note: Primary key columns of different types cannot be compared.</p>
     * <p>{@link #INF_MIN} and {@link #INF_MAX} can be compared with other types of primary key columns,
     * and {@link #INF_MIN} is smaller than any value of any primary key column type, 
     * while {@link #INF_MAX} is greater than any value of any primary key column type.</p>
     *
     * @param target
     * @return If greater, the return value is greater than 0; if equal, returns 0; if less, the return value is less than 0
     */
    @Override
    public int compareTo(PrimaryKeyValue target) {
        if (this.type == null) { // INF_MIN or INF_MAX or AUTO_INCR
            if (target.type == null && target.value.equals(this.value)) {
                return 0;
            }

            if (this.value.equals("INF_MIN")) {
                return -1;
            } else if (this.value.equals("INF_MAX")){
                return 1;
            } else {
                throw new IllegalArgumentException(this.value + " can't compare.");
            } 
        } else {
            if (target.type == null) {
                if (target.value.equals("INF_MIN")) {
                    return 1;
                } else {
                    return -1;
                }
            }

            if (this.type != target.type) {
                throw new IllegalArgumentException("The type of primary key to compare must be the same.");
            }

            switch (this.type) {
                case STRING:
                    return ((String) value).compareTo(target.asString());
                case INTEGER:
                    return ((Long) value).compareTo(target.asLong());
                case BINARY:
                    byte[] b1 = (byte[]) this.value;
                    byte[] b2 = (byte[]) target.value;
                    return Bytes.compareByteArrayInLexOrder(b1, 0, b1.length, b2, 0, b2.length);
                default:
                    throw new IllegalArgumentException("Unknown type: " + this.type);
            }
        }
    }

    public static PrimaryKeyValue addOne(PrimaryKeyValue target) {
        if (target.getType() == null) {
            throw new IllegalArgumentException("cannot addOne for null type");
        }
        ColumnValue value = null;
        switch (target.getType()) {
            case INTEGER:
                value = ColumnValue.fromLong(target.asLong());
                if (value.asLong() == Long.MAX_VALUE) {
                    return new PrimaryKeyValue("INF_MAX", null);
                }
                Long integerVal = value.asLong() + 1;
                return new PrimaryKeyValue(integerVal, PrimaryKeyType.INTEGER);
            case STRING:
                value = ColumnValue.fromString(target.asString());
                String stringVal = value.asString() + "\0";
                return new PrimaryKeyValue(stringVal, PrimaryKeyType.STRING);
            case BINARY:
                value = ColumnValue.fromBinary(target.asBinary());
                byte[] binaryVal = new byte[value.getDataSize() + 1];
                System.arraycopy(value.asBinary(), 0, binaryVal, 0, value.getDataSize());
                binaryVal[value.getDataSize()] = 0;
                return new PrimaryKeyValue(binaryVal, PrimaryKeyType.BINARY);
            default:
                throw new IllegalArgumentException("Unknown type: " + target.getType());
        }
    }
}
