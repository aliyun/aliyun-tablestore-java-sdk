package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 表的读写吞吐量的单位，即能力单元。
 * 主要用于{@link ReservedThroughput}中配置表的预留读写吞吐量以及标识读写操作消耗的能力单元的量。
 */
public class CapacityDataSize implements Jsonizable {
    /**
     * 读能力单元。
     */
    private OptionalValue<Long> readCapacityDataSize = new OptionalValue<Long>("ReadCapacityDataSize");

    /**
     * 写能力单元。
     */
    private OptionalValue<Long> writeCapacityDataSize = new OptionalValue<Long>("WriteCapacityDataSize");

    /**
     * 默认构造函数。
     * <p>读能力单元和写能力单元默认为未设置。</p>
     */
    public CapacityDataSize() {
    }

    /**
     * 构造CapacityUnit对象，并指定读能力单元的值和写能力单元的值。
     *
     * @param readCapacityDataSize  读能力单元的值，必须大于等于0。
     * @param writeCapacityDataSize 写能力单元的值，必须大于等于0。
     * @throws IllegalArgumentException 若读或写能力单元值为负数。
     */
    public CapacityDataSize(long readCapacityDataSize, long writeCapacityDataSize) {
        setReadCapacityDataSize(readCapacityDataSize);
        setWriteCapacityDataSize(writeCapacityDataSize);
    }
    /**
     * 获取读能力单元的值。
     *
     * @return 读能力单元的值。
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public long getReadCapacityDataSize() {
        if (!readCapacityDataSize.isValueSet()) {
            throw new IllegalStateException("The value of read capacity unit is not set.");
        }
        return readCapacityDataSize.getValue();
    }

    /**
     * 设置读能力单元的值，设置的值必须大于等于0。
     *
     * @param readCapacityDataSize 读能力单元的值
     * @throws IllegalArgumentException 若读能力单元的值为负数。
     */
    public void setReadCapacityDataSize(long readCapacityDataSize) {
        Preconditions.checkArgument(readCapacityDataSize >= 0, "The value of read capacity DataSize can't be negative.");
        this.readCapacityDataSize.setValue(readCapacityDataSize);
    }

    /**
     * 查询是否设置了读能力单元。
     *
     * @return 是否有设置读能力单元
     */
    public boolean hasSetReadCapacityDataSize() {
        return readCapacityDataSize.isValueSet();
    }

    /**
     * 清除设置的读CapacityDataSize。
     */
    public void clearReadCapacityDataSize() {
        readCapacityDataSize.clear();
    }

    /**
     * 获取写能力单元的值。
     *
     * @return 写能力单元的值。
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public long getWriteCapacityDataSize() {
        if (!writeCapacityDataSize.isValueSet()) {
            throw new IllegalStateException("The value of write capacity unit is not set.");
        }
        return writeCapacityDataSize.getValue();
    }

    /**
     * 设置写能力单元的值，设置的值必须大于等于0。
     *
     * @param writeCapacityDataSize 写能力单元的值
     * @throws IllegalArgumentException 若写能力单元的值为负数。
     */
    public void setWriteCapacityDataSize(long writeCapacityDataSize) {
        Preconditions.checkArgument(writeCapacityDataSize >= 0, "The value of write capacity unit can't be negative.");
        this.writeCapacityDataSize.setValue(writeCapacityDataSize);
    }

    /**
     * 查询是否设置了写能力单元。
     *
     * @return 是否有设置写能力单元
     */
    public boolean hasSetWriteCapacityDataSize() {
        return writeCapacityDataSize.isValueSet();
    }

    /**
     * 清除设置的写CapacityUnit。
     */
    public void clearWriteCapacityDataSize() {
        writeCapacityDataSize.clear();
    }

    @Override
    public int hashCode() {
        return readCapacityDataSize.hashCode() ^ writeCapacityDataSize.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof CapacityDataSize)) {
            return false;
        }

        CapacityDataSize c1 = (CapacityDataSize) o;
        return this.readCapacityDataSize.equals(c1.readCapacityDataSize) && this.writeCapacityDataSize.equals(c1.writeCapacityDataSize);
    }

    @Override
    public String toString() {
        return "" + readCapacityDataSize + ", " + writeCapacityDataSize;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        boolean firstItem = true;
        sb.append('{');
        if (readCapacityDataSize.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Read_Size\": ");
            sb.append(readCapacityDataSize.getValue());
        }
        if (writeCapacityDataSize.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Write_Size\": ");
            sb.append(writeCapacityDataSize.getValue());
        }
        sb.append('}');
    }
}