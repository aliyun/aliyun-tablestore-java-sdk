package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * 表的读写吞吐量的单位，即能力单元。
 * 主要用于{@link ReservedThroughput}中配置表的预留读写吞吐量以及标识读写操作消耗的能力单元的量。
 */
public class CapacityUnit implements Jsonizable {
    /**
     * 读能力单元。
     */
    private OptionalValue<Integer> readCapacityUnit = new OptionalValue<Integer>("ReadCapacityUnit");

    /**
     * 写能力单元。
     */
    private OptionalValue<Integer> writeCapacityUnit = new OptionalValue<Integer>("WriteCapacityUnit");

    /**
     * 默认构造函数。
     * <p>读能力单元和写能力单元默认为未设置。</p>
     */
    public CapacityUnit() {
    }

    /**
     * 构造CapacityUnit对象，并指定读能力单元的值和写能力单元的值。
     *
     * @param readCapacityUnit  读能力单元的值，必须大于等于0。
     * @param writeCapacityUnit 写能力单元的值，必须大于等于0。
     * @throws IllegalArgumentException 若读或写能力单元值为负数。
     */
    public CapacityUnit(int readCapacityUnit, int writeCapacityUnit) {
        setReadCapacityUnit(readCapacityUnit);
        setWriteCapacityUnit(writeCapacityUnit);
    }

    /**
     * 获取读能力单元的值。
     *
     * @return 读能力单元的值。
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getReadCapacityUnit() {
        if (!readCapacityUnit.isValueSet()) {
            throw new IllegalStateException("The value of read capacity unit is not set.");
        }
        return readCapacityUnit.getValue();
    }

    /**
     * 设置读能力单元的值，设置的值必须大于等于0。
     *
     * @param readCapacityUnit 读能力单元的值
     * @throws IllegalArgumentException 若读能力单元的值为负数。
     */
    public void setReadCapacityUnit(int readCapacityUnit) {
        Preconditions.checkArgument(readCapacityUnit >= 0, "The value of read capacity unit can't be negative.");
        this.readCapacityUnit.setValue(readCapacityUnit);
    }

    /**
     * 查询是否设置了读能力单元。
     *
     * @return 是否有设置读能力单元
     */
    public boolean hasSetReadCapacityUnit() {
        return readCapacityUnit.isValueSet();
    }

    /**
     * 清除设置的读CapacityUnit。
     */
    public void clearReadCapacityUnit() {
        readCapacityUnit.clear();
    }
    
    /**
     * 获取写能力单元的值。
     *
     * @return 写能力单元的值。
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getWriteCapacityUnit() {
        if (!writeCapacityUnit.isValueSet()) {
            throw new IllegalStateException("The value of write capacity unit is not set.");
        }
        return writeCapacityUnit.getValue();
    }

    /**
     * 设置写能力单元的值，设置的值必须大于等于0。
     *
     * @param writeCapacityUnit 写能力单元的值
     * @throws IllegalArgumentException 若写能力单元的值为负数。
     */
    public void setWriteCapacityUnit(int writeCapacityUnit) {
        Preconditions.checkArgument(writeCapacityUnit >= 0, "The value of write capacity unit can't be negative.");
        this.writeCapacityUnit.setValue(writeCapacityUnit);
    }

    /**
     * 查询是否设置了写能力单元。
     *
     * @return 是否有设置写能力单元
     */
    public boolean hasSetWriteCapacityUnit() {
        return writeCapacityUnit.isValueSet();
    }

    /**
     * 清除设置的写CapacityUnit。
     */
    public void clearWriteCapacityUnit() {
        writeCapacityUnit.clear();
    }

    @Override
    public int hashCode() {
        return readCapacityUnit.hashCode() ^ writeCapacityUnit.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof CapacityUnit)) {
            return false;
        }

        CapacityUnit c1 = (CapacityUnit) o;
        return this.readCapacityUnit.equals(c1.readCapacityUnit) && this.writeCapacityUnit.equals(c1.writeCapacityUnit);
    }

    @Override
    public String toString() {
        return "" + readCapacityUnit + ", " + writeCapacityUnit;
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
        if (readCapacityUnit.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Read\": ");
            sb.append(readCapacityUnit.getValue());
        }
        if (writeCapacityUnit.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Write\": ");
            sb.append(writeCapacityUnit.getValue());
        }
        sb.append('}');
    }
}
