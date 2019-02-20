package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.*;

import java.io.IOException;

/**
 * TableStore中每行数据都包含主键({@link PrimaryKey})，
 * 主键由多列主键列构成({@link PrimaryKeyColumn})，
 * 每一个主键列包含主键列名称和主键列的值{@link PrimaryKeyValue}。
 */
public class PrimaryKeyColumn implements Comparable<PrimaryKeyColumn>, Jsonizable, Measurable {
    /**
     * 主键列的名称。
     */
    private String name;

    /**
     * 主键列的值。
     */
    private PrimaryKeyValue value;

    /**
     * 序列化后占用的数据大小
     */
    private int dataSize = -1;

    /**
     * 根据指定的主键列的名称和主键列的值构造主键列。
     * <p>主键列的名称不能为null pointer及空字符串。</p>
     * <p>主键列的值不能为null pointer。</p>
     *
     * @param name  主键列的名称
     * @param value 主键列的值
     */
    public PrimaryKeyColumn(String name, PrimaryKeyValue value) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of primary key should not be null.");

        this.name = name;
        this.value = value;
    }

    /**
     * 获取主键列的名称。
     *
     * @return 主键列的名称
     */
    public String getName() {
        return name;
    }

    public byte[] getNameRawData() {
        return Bytes.toBytes(name);
    }

    /**
     * 获取主键列的值。
     *
     * @return 主键列的值
     */
    public PrimaryKeyValue getValue() {
        return value;
    }

    /**
     * 将主键列类型转化为属性列类型。
     * @return
     */
    public Column toColumn() throws IOException {
        return new Column(getName(), getValue().toColumnValue());
    }

    @Override
    public String toString() {
        return "'" + name + "':" + value;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ value.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKeyColumn)) {
            return false;
        }

        PrimaryKeyColumn col = (PrimaryKeyColumn) o;
        return this.name.equals(col.name) && this.value.equals(col.value);
    }

    /**
     * 比较两个主键列的大小。
     * <p>对比的两个主键列必须含有相同的名称和类型。</p>
     *
     * @param target
     * @return 若相等返回0，若大于返回1，若小于返回-1
     */
    @Override
    public int compareTo(PrimaryKeyColumn target) {
        if (!this.name.equals(target.name)) {
            throw new IllegalArgumentException("The name of primary key to be compared must be the same.");
        }

        return this.value.compareTo(target.value);
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
        sb.append("\", \"Type\": \"");
        if ( value.getType() != null ) {
            sb.append(value.getType().toString());
            sb.append("\", \"Value\": ");
            switch(value.getType()) {
            case INTEGER:
                sb.append(value.asLong());
                break;
            case STRING:
                sb.append("\"");
                sb.append(value.asString());
                sb.append("\"");
                break;
            case BINARY:
                sb.append("\"");
                sb.append(Base64.toBase64String(value.asBinary()));
                sb.append("\"");
                break;
            default:
                throw new IllegalArgumentException("Unknown type: " + value.getType());
            }
        } else {
            sb.append("null");
            sb.append("\", \"Value\": ");
            if ( value.isInfMin() ) {
                sb.append("\"");
                sb.append("INF_MIN");
                sb.append("\"");
            } else if ( value.isInfMax() ) {
                sb.append("\"");
                sb.append("INF_MAX");
                sb.append("\"");
            } else {
                throw new IllegalArgumentException("Unknown value: " + value.asString());
            }
        }
        sb.append("}");
    }

    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            dataSize = CalculateHelper.calcStringSizeInBytes(name) + value.getDataSize();
        }

        return dataSize;
    }

}
