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
 * 表示属性列的值。
 */
public class ColumnValue implements Comparable<ColumnValue>, Jsonizable, Measurable {

    /**
     * 只供内部使用，请勿使用。
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
     * 获取主键列值的大小，各类型大小计算公式为：
     *  - {@link ColumnType#INTEGER}: 恒定大小为8个字节
     *  - {@link ColumnType#DOUBLE}: 恒定大小为8个字节
     *  - {@link ColumnType#BOOLEAN}: 恒定大小为1个字节
     *  - {@link ColumnType#BINARY}: 大小为字节数
     *  - {@link ColumnType#STRING}: 大小为按UTF-8编码后的字节数
     *
     * @return 值的大小
     */
    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            dataSize = calculateDataSize();
        }
        return this.dataSize;
    }

    /**
     * 获取属性列的类型。
     *
     * @return 属性列的类型。
     */
    public ColumnType getType() {
        return type;
    }

    /**
     * 构造一个类型为{@link ColumnType#STRING}的属性列。
     * <p>注意：值不能为null pointer。</p>
     *
     * @param value 字符串类型的值。
     * @return 生成的实例
     */
    public static ColumnValue fromString(String value) {
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        return new ColumnValue(value, ColumnType.STRING);
    }

    /**
     * 构造一个类型为{@link ColumnType#INTEGER}的属性列。
     *
     * @param value 长整型的值。
     * @return 生成的实例
     */
    public static ColumnValue fromLong(long value) {
        return new ColumnValue(value, ColumnType.INTEGER);
    }

    /**
     * 构造一个类型为{@link ColumnType#BINARY}的属性列。
     * <p>注意：值不能为null pointer。</p>
     *
     * @param value 二进制字符串类型的值。
     * @return 生成的实例
     */
    public static ColumnValue fromBinary(byte[] value) {
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        return new ColumnValue(value, ColumnType.BINARY);
    }

    /**
     * 构造一个类型为{@link ColumnType#DOUBLE}的属性列。
     *
     * @param value double类型的值。
     * @return 生成的实例
     */
    public static ColumnValue fromDouble(double value) {
        return new ColumnValue(value, ColumnType.DOUBLE);
    }

    /**
     * 构造一个类型为{@link ColumnType#BOOLEAN}的属性列。
     *
     * @param value 布尔类型的值。
     * @return 生成的实例
     */
    public static ColumnValue fromBoolean(boolean value) {
        return new ColumnValue(value, ColumnType.BOOLEAN);
    }

    /**
     * 获取属性列的字符串类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#STRING}才能获取到值。</p>
     *
     * @return 字符串类型的值
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
     * 获取属性列的长整型类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#INTEGER}才能获取到值。</p>
     *
     * @return 长整型值
     */
    public long asLong() {
        if (this.type != ColumnType.INTEGER) {
            throw new IllegalStateException("The type of column is not INTEGER.");
        }

        return (Long) value;
    }

    /**
     * 获取属性列的二进制字符串类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BINARY}才能获取到值。</p>
     *
     * @return 二进制字符串类型的值
     */
    public byte[] asBinary() {
        if (this.type != ColumnType.BINARY) {
            throw new IllegalStateException("The type of column is not BINARY.");
        }
        return (byte[]) value;
    }

    /**
     * 获取属性列的DOUBLE类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#DOUBLE}才能获取到值。</p>
     *
     * @return Double类型的值
     */
    public double asDouble() {
        if (this.type != ColumnType.DOUBLE) {
            throw new IllegalStateException("The type of column is not DOUBLE.");
        }
        return (Double) value;
    }

    /**
     * 获取属性列的布尔类型的值。
     * <p>当前仅当数据类型为{@link ColumnType#BOOLEAN}才能获取到值。</p>
     *
     * @return 布尔类型的值
     */
    public boolean asBoolean() {
        if (this.type != ColumnType.BOOLEAN) {
            throw new IllegalStateException("The type of column is not BOOLEAN.");
        }
        return (Boolean) value;
    }

    /**
     * 采用crc8算法得到一个checksum，主要用于计算cell的checksum
     * @param crc crc初始值
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
     * 比较两个属性列的值的大小。
     * <p>注意：不同类型的属性列无法比较。</p>
     *
     * @param target
     * @return 如果大于返回值大于0，等于返回0，小于返回值小于0
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

