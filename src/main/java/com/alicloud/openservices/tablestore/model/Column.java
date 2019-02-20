package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.*;

/**
 * TableStore中每行数据可以包含一个或多个属性列({@link Column})，
 * 每一个属性列包含名称、值{@link ColumnValue}以及时间戳。
 */
public class Column implements Jsonizable, Measurable {
    public static NameTimestampComparator NAME_TIMESTAMP_COMPARATOR = new NameTimestampComparator();

    /**
     * 属性列的名称。
     */
    private String name;
    /**
     * 属性列的值。
     */
    private ColumnValue value;

    /**
     * 属性列的时间戳。
     */
    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * 序列化后占用的数据大小
     */
    private int dataSize = -1;

    /**
     * 构造一个属性列，必须包含名称、值和时间戳。
     * <p>属性列的名称不能为null pointer及空字符串。</p>
     * <p>属性列的值不能为null pointer。</p>
     *
     * @param name      属性列的名称
     * @param value     属性列的值
     * @param timestamp 属性列的时间戳
     */
    public Column(String name, ColumnValue value, long timestamp) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of column should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        Preconditions.checkArgument(timestamp >= 0, "The timestamp should not be negative.");

        this.name = name;
        this.value = value;
        this.timestamp.setValue(timestamp);
    }

    public Column(String name, ColumnValue value) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of column should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of column should not be null.");

        this.name = name;
        this.value = value;
    }

    /**
     * 获取属性列的名称。
     *
     * @return 属性列的名称
     */
    public String getName() {
        return name;
    }

    public byte[] getNameRawData() {
        return Bytes.toBytes(name);
    }

    /**
     * 获取属性列的值。
     *
     * @return 属性列的值
     */
    public ColumnValue getValue() {
        return value;
    }

    /**
     * 获取属性列的时间戳。
     *
     * @return 属性列的时间戳
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public long getTimestamp() {
        if (!timestamp.isValueSet()) {
            throw new IllegalStateException("The value of Timestamp is not set.");
        }
        return timestamp.getValue();
    }

    /**
     * 检查是否设置了时间戳。
     *
     * @return 若设置了时间戳，则返回true，否则返回false
     */
    public boolean hasSetTimestamp() {
        return timestamp.isValueSet();
    }

    @Override
    public String toString() {
        return "Name:" + name + ",Value:" + value + "," + timestamp;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ value.hashCode() ^ timestamp.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Column)) {
            return false;
        }

        Column col = (Column) o;
        return this.name.equals(col.name) && this.value.equals(col.value) && this.timestamp.equals(col.timestamp);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Name\": \"");
        sb.append(name);
        sb.append("\", ");

        if (timestamp.isValueSet()) {
            sb.append("\"Timestamp\": ");
            sb.append(timestamp.getValue());
            sb.append(", ");
        }

        sb.append("\"Value\": ");
        value.jsonize(sb, newline + "  ");
        sb.append("}");
    }

    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            int size = CalculateHelper.calcStringSizeInBytes(name) + value.getDataSize();
            if (hasSetTimestamp()) {
                size += 8;
            }
            dataSize = size;
        }
        return dataSize;
    }
}
