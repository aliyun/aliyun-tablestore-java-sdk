package com.aliyun.openservices.ots.model;

import java.util.Arrays;
import com.aliyun.openservices.ots.utils.Bytes;
import com.aliyun.openservices.ots.utils.CalculateHelper;

import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;
import static com.aliyun.openservices.ots.utils.CodingUtils.*;

/**
 * 表示行中数据列的值。
 * 
 */
public class ColumnValue implements Comparable<ColumnValue> {
    private ColumnType type; // 类型。
    private Object value; // 值。
    private int dataSize = 0;
    
    private ColumnValue(Object value, ColumnType type){
        assertParameterNotNull(value, "value");
        assertParameterNotNull(type, "type");
        this.value = value;
        this.type = type;
        switch (this.type) {
            case INTEGER:
                this.dataSize = 8;
                break;
            case STRING:
                this.dataSize = CalculateHelper.getStringDataSize(this.asString());
                break;
            case BINARY:
                this.dataSize = this.asBinary().length;
                break;
            case DOUBLE:
                this.dataSize = 8;
                break;
            case BOOLEAN:
                this.dataSize = 1;
                break;
            default:
                throw new IllegalStateException("Bug: not support the type : " + type);
        }
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
    public int getSize() {
        return this.dataSize;
    }

    /**
     * 获取值的数据类型。
     * @return 值的数据类型。
     */
    public ColumnType getType() {
        return this.type;
    }

    /**
     * 使用<code>int</code>对象构造值的数据类型为<code>ColumnType.INTEGER</code>的ColumnValue对象。
     * @param value
     *          <code>int</code>值。
     */
    public static ColumnValue fromLong(long value) {
        return new ColumnValue(value, ColumnType.INTEGER);
    }

    /**
     * 使用<code>boolean</code>值构造值的数据类型为<code>ColumnType.BOOLEAN</code>的ColumnValue对象。
     * @param value
     *          <code>boolean</code>值。
     */
    public static ColumnValue fromBoolean(boolean value) {
        return new ColumnValue(value, ColumnType.BOOLEAN);
    }

    /**
     * 使用<code>double</code>对象构造值的数据类型为<code>ColumnType.DOUBLE</code>的ColumnValue对象。
     * @param value
     *          <code>double</code>值。
     */
    public static ColumnValue fromDouble(double value) {
        if (Double.isNaN(value)) {
            throw new IllegalArgumentException(
                    OTS_RESOURCE_MANAGER.getString("DoubleNaNNotSupported"));
        }
    
        return new ColumnValue(value, ColumnType.DOUBLE);
    }
    
    public static ColumnValue fromBinary(byte[] value) {
        assertParameterNotNull(value, "value");
        return new ColumnValue(value, ColumnType.BINARY);
    }
    
    /**
     * 转换为字符串类型。
     * 当且仅当数据类型为<code>ColumnType.STRING</code>时转换能够成功。
     * @return <code>String</code>值。
     */
    public String asString() {
        if (this.type != ColumnType.STRING) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "STRING"));
        }
        
        return this.value.toString();
    }
    
    /**
     * 使用<code>String</code>对象构造值的数据类型为<code>ColumnType.STRING</code>的ColumnValue对象。
     * @param value
     *          <code>String</code>对象。
     */
    public static ColumnValue fromString(String value) {
        assertParameterNotNull(value, "value");
        return new ColumnValue(value, ColumnType.STRING);
    }

    /**
     * 转换为长整型。
     * 当且仅当数据类型为<code>ColumnType.INTEGER</code>时转换能够成功。
     * @return <code>long</code>值。
     */
    public long asLong() {
        if (this.type != ColumnType.INTEGER) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "INTEGER"));
        }

        return (Long)value;
    }

    /**
     * 转换为双整型。
     * 当且仅当数据类型为<code>ColumnType.DOUBLE</code>时转换能够成功。
     * @return <code>double</code>值。
     */
    public double asDouble() {
        if (this.type != ColumnType.DOUBLE) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "DOUBLE"));
        }

        Double result = (Double)value;
        // Handle the precision difference between java and OTS server.
        if (result == Double.POSITIVE_INFINITY) {
            return Double.MAX_VALUE;
        } else if (result == Double.NEGATIVE_INFINITY){
            return -Double.MAX_VALUE;
        }
        else{
            return result;
        }
    }
    /**
     * 转换为布尔型。
     * 当且仅当数据类型为<code>ColumnType.BOOLEAN</code>时转换能够成功。
     * @return <code>boolean</code>值。
     */
    public boolean asBoolean() {
        if (this.type != ColumnType.BOOLEAN) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "BOOLEAN"));
        }

        return (Boolean)value;
    }
    
    /**
     * 转换为二进制类型。
     * 当且仅当数据类型为<code>ColumnType.BINARY</code>时转换能够成功。
     * @return <code>byte[]</code>值。
     */
    public byte[] asBinary() {
        if (this.type != ColumnType.BINARY) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "BINARY"));
        }
        return (byte[])value;
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
                return this.value.equals(val.value);
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
        if (this.type == ColumnType.BINARY) {
            return Arrays.toString(asBinary()) + ":" + this.type;
        } else {
            return this.value.toString() + ":" + this.type;
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
}
