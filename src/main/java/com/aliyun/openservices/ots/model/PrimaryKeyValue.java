package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.utils.Bytes;
import com.aliyun.openservices.ots.utils.CalculateHelper;

import java.util.Arrays;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;

/**
 * 表示主键（PrimaryKey）列的值。
 *
 */
public class PrimaryKeyValue implements Comparable<PrimaryKeyValue>  {

    /**
     * 表示主键值范围的最大值。
     */
    public static final PrimaryKeyValue INF_MAX = new PrimaryKeyValue("INF_MAX", null);
    /**
     * 表示主键值范围的最小值。
     */
    public static final PrimaryKeyValue INF_MIN = new PrimaryKeyValue("INF_MIN", null);
    
    private Object value; 
    private PrimaryKeyType type;
    private int dataSize = 0;

    /**
     * 构造函数。
     * 对用户隐藏该构造函数，以防止用户提供错误的值。比如类型指定为INTEGER，但值并非合法的表示整数的字符串。
     * @param value
     *          主键值。
     * @param type
     *          值的数据类型。
     */
    PrimaryKeyValue(Object value, PrimaryKeyType type){
        this.value = value;
        this.type = type;

        if (this.type != null) {
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
                default:
                    throw new IllegalStateException("Bug: not support the type : " + type);
            }
        }
    }

    /**
     * 获取值的数据类型。
     * @return 值的数据类型。
     */
    public PrimaryKeyType getType() {
        return type;
    }

    /**
     * 获取主键列值的大小，各类型大小计算公式为：
     *  - {@link PrimaryKeyType#INTEGER}: 恒定大小为8个字节
     *  - {@link PrimaryKeyType#STRING}: 大小为按UTF-8编码后的字节数
     *
     * @return 值的大小
     */
    public int getSize() {
        return this.dataSize;
    }

    /**
     * 使用<code>String</code>对象构造值的数据类型为{@link PrimaryKeyType#STRING}的
     * {@link PrimaryKeyValue}对象。
     * @param value
     *          <code>String</code>对象。
     */
    public static PrimaryKeyValue fromString(String value){
        assertParameterNotNull(value, "value");

        return new PrimaryKeyValue(value, PrimaryKeyType.STRING);
    }

    /**
     * 使用<code>int</code>值构造值的数据类型为{@link PrimaryKeyType#INTEGER}的
     * {@link PrimaryKeyValue}对象。
     * @param value
     *          <code>int</code>值。
     */
    public static PrimaryKeyValue fromLong(long value){
        return new PrimaryKeyValue(value, PrimaryKeyType.INTEGER);
    }
    
    public String asString() {
        if (this.type != PrimaryKeyType.STRING){
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "STRING"));
        }

        return (String)value;
    }
    
    /**
     * 转换为长整型。
     * 当且仅当数据类型为{@link PrimaryKeyType#INTEGER}时转换能够成功。
     * @return long 长整数值。
     */
    public long asLong() {
        if (this.type != PrimaryKeyType.INTEGER){
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "INTEGER"));
        }

        return (Long)value;
    }

    public static PrimaryKeyValue fromBinary(byte[] value) {
        return new PrimaryKeyValue(value, PrimaryKeyType.BINARY);
    }

    public byte[] asBinary() {
        if (this.type != PrimaryKeyType.BINARY) {
            throw new IllegalStateException(
                    OTS_RESOURCE_MANAGER.getFormattedString("DateTypeIsNot", "BINARY"));
        }
        return (byte[])value;
    }

    public boolean isInfMin() {
        return type == null && value.equals("INF_MIN");
    }

    public boolean isInfMax() {
        return type == null && value.equals("INF_MAX");
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

    @Override
    public int hashCode() {
        if (this.type == PrimaryKeyType.BINARY) {
            return Arrays.hashCode(asBinary()) * 31 + this.type.hashCode();
        } else {
            return this.value.hashCode() * 31 + this.type.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKeyValue)) {
            return false;
        }

        PrimaryKeyValue val = (PrimaryKeyValue)o;
        if (isInfMin()) {
            return val.isInfMin();
        }

        if (isInfMax()) {
            return val.isInfMax();
        }

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
    public String toString() {
        if (this.type == PrimaryKeyType.BINARY) {
            return Arrays.toString(asBinary()) + ":" + this.type;
        } else {
            return this.value.toString() + ":" + this.type;
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
        if (this.type == null) { // INF_MIN or INF_MAX
            if (target.type == null && target.value.equals(this.value)) {
                return 0;
            }

            if (this.value.equals("INF_MIN")) {
                return -1;
            } else {
                return 1;
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
                    int ret = Bytes.compareByteArrayInLexOrder(b1, 0, b1.length, b2, 0, b2.length);
                    return ret;
                default:
                    throw new IllegalArgumentException("Unknown type: " + this.type);
            }
        }
    }
}
