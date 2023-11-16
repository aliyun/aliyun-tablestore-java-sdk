package com.alicloud.openservices.tablestore.model;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Arrays;

import com.alicloud.openservices.tablestore.core.protocol.PlainBufferCrc8;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;

import static com.alicloud.openservices.tablestore.core.protocol.PlainBufferConsts.*;

/**
 * 表示主键列的值。
 * <p>若要构造{@link PrimaryKeyType#INTEGER}类型的主键列，请使用{@link #fromLong(long)}来初始化。</p>
 * <p>若要构造{@link PrimaryKeyType#STRING}类型的主键列，请使用{@link #fromString(String)}来初始化。</p>
 * <p>若要构造{@link PrimaryKeyType#BINARY}类型的主键列，请使用{@link #fromBinary(byte[])}来初始化。</p>
 * <p>若要构造{@link PrimaryKeyType#DATETIME}类型的主键列，请使用{@link #fromDateTime(ZonedDateTime)}来初始化。</p>
 * <p>注意：{@link #INF_MIN}和{@link #INF_MAX}是特殊的主键列，其唯一的用途是用于{@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)}
 * 操作中表示主键列的范围，不能作为实际的数据写入TableStore，也不能作为除GetRange操作之外的读操作的参数。</p>
 */
public class PrimaryKeyValue implements Comparable<PrimaryKeyValue>, Measurable {

    /**
     * 表示主键值范围的最大值，其唯一的用途是用于{@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)}
     * 操作中表示主键列的范围，不能作为实际的数据写入TableStore，也不能作为除GetRange操作之外的读操作的参数。
     */
    public static final PrimaryKeyValue INF_MAX = new PrimaryKeyValue("INF_MAX", null);
    /**
     * 表示主键值范围的最小值，其唯一的用途是用于{@link com.alicloud.openservices.tablestore.SyncClientInterface#getRange(GetRangeRequest)}
     * 操作中表示主键列的范围，不能作为实际的数据写入TableStore，也不能作为除GetRange操作之外的读操作的参数。
     */
    public static final PrimaryKeyValue INF_MIN = new PrimaryKeyValue("INF_MIN", null);
    
    /**
     * 表示主键值保留，其唯一的用途是用于PK递增列的填充。
     * 当某一PK列被设置为递增列，则在PutRow或UpdateRow时，用户指定这列的值为AUTO_INCREMENT，则OTS会在这行写入成功后自动赋予一个更大值，保证此列的值是永久递增的。
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
                case DATETIME:
                    this.dataSize = 8;
                    break;
                default:
                    throw new IllegalStateException("Bug: not support the type : " + type);
            }
        }
    }

    /**
     * 获取主键列的类型。
     *
     * @return 主键列的类型。
     */
    public PrimaryKeyType getType() {
        return type;
    }

    @Deprecated
    public int getSize() {
        return getDataSize();
    }

    /**
     * 获取主键列值的大小，各类型大小计算公式为：
     *  - {@link PrimaryKeyType#INTEGER}: 恒定大小为8个字节
     *  - {@link PrimaryKeyType#STRING}: 大小为按UTF-8编码后的字节数
     *  - {@link PrimaryKeyType#DATETIME}: 存储为8个字节
     * @return 值的大小
     */
    @Override
    public int getDataSize() {
        return this.dataSize;
    }

    /**
     * 构造一个类型为{@link PrimaryKeyType#STRING}的主键列。
     * <p>注意：值不能为null pointer。</p>
     *
     * @param value 字符串类型的值。
     * @return 生成的对象
     */
    public static PrimaryKeyValue fromString(String value) {
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");
        return new PrimaryKeyValue(value, PrimaryKeyType.STRING);
    }

    /**
     * 构造一个类型为{@link PrimaryKeyType#DATETIME}的主键列。
     *
     * @param value java.time.ZonedDateTime类型的对象。
     * @return 生成的对象
     */
    public static PrimaryKeyValue fromDateTime(ZonedDateTime value) {
        return new PrimaryKeyValue(ValueUtil.parseDateTimeToMicroTimestamp(value), PrimaryKeyType.DATETIME);
    }

    /**
     * 构造一个类型为{@link PrimaryKeyType#INTEGER}的主键列。
     *
     * @param value 长整型的值。
     * @return 生成的对象
     */
    public static PrimaryKeyValue fromLong(long value) {
        return new PrimaryKeyValue(value, PrimaryKeyType.INTEGER);
    }

    /**
     * 构造一个类型为{@link PrimaryKeyType#BINARY}的主键列。
     * <p>注意：值不能为null pointer。</p>
     *
     * @param value 二进制字符串类型的值。
     * @return 生成的对象
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
            case DATETIME:
                return fromDateTime(value.asDateTime());
            default:
                throw new IllegalArgumentException("Can not convert from column with not compatible type: " + value.getType());
        }
    }

    /**
     * 采用crc8算法得到一个checksum，主要用于计算cell的checksum
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
            case DATETIME:{
                crc = PlainBufferCrc8.crc8(crc, VT_DATETIME);
                crc = PlainBufferCrc8.crc8(crc, ValueUtil.parseDateTimeToMicroTimestamp(asDateTime()));
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
            case DATETIME:{
                value = ColumnValue.fromDateTime(asDateTime());
                break;
            }
            default:
                throw new IOException("Unsupported primary key type: " + getType());
        }
        return value;
    }

    /**
     * 获取主键列的字符串类型的值。
     * <p>当前仅当数据类型为{@link PrimaryKeyType#STRING}才能获取到值。</p>
     *
     * @return 字符串类型的值
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
     * 获取主键列的长整型类型的值。
     * <p>当前仅当数据类型为{@link PrimaryKeyType#INTEGER}才能获取到值。</p>
     *
     * @return 长整型值
     */
    public long asLong() {
        if (this.type != PrimaryKeyType.INTEGER) {
            throw new IllegalStateException("The type of primary key is not INTEGER.");
        }

        return (Long) value;
    }

    /**
     * 获取主键列的二进制字符串类型的值。
     * <p>当前仅当数据类型为{@link PrimaryKeyType#BINARY}才能获取到值。</p>
     *
     * @return 二进制字符串类型的值
     */
    public byte[] asBinary() {
        if (this.type != PrimaryKeyType.BINARY) {
            throw new IllegalStateException("The type of primary key is not BINARY");
        }
        return (byte[]) value;
    }

    /**
     * 获取主键列的时间类型的值。
     * <p>当前仅当数据类型为{@link PrimaryKeyType#DATETIME}才能获取到值。</p>
     *
     * @return java.time.ZonedDateTime类型的值
     */
    public ZonedDateTime asDateTime() {
        if (this.type != PrimaryKeyType.DATETIME) {
            throw new IllegalStateException("The type of primary key is not DATETIME.");
        }
        return ValueUtil.parseMicroTimestampToUTCDateTime((Long) value);
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
     * 比较两个主键列的值的大小。
     * <p>注意：不同类型的主键列无法比较。</p>
     * <p>{@link #INF_MIN}和{@link #INF_MAX}可以与其他类型的主键列进行比较，
     * 并且{@link #INF_MIN}比任何类型的主键列值小，{@link #INF_MAX}比任何类型的主键列值大。</p>
     *
     * @param target
     * @return 如果大于返回值大于0，等于返回0，小于返回值小于0
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
                case DATETIME:
                    return ((Long) value).compareTo(ValueUtil.parseDateTimeToMicroTimestamp(target.asDateTime()));
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
            case DATETIME:
                value = ColumnValue.fromDateTime(target.asDateTime());
                long ts = ValueUtil.parseDateTimeToMicroTimestamp(value.asDateTime()) + 1;
                if (ts == Long.MAX_VALUE) {
                    return new PrimaryKeyValue("INF_MAX", null);
                }
                return new PrimaryKeyValue(ts, PrimaryKeyType.DATETIME);
            default:
                throw new IllegalArgumentException("Unknown type: " + target.getType());
        }
    }
}
