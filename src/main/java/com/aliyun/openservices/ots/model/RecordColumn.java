package com.aliyun.openservices.ots.model;


public class RecordColumn {

    public enum ColumnType {
        /**
         * PUT类型
         * 表示写入该列
         */
        PUT,

        /**
         * DELETE类型
         * 表示删除该列
         */
        DELETE
    }

    /**
     * 列名
     */
    private String name;

    /**
     * 列值
     */
    private ColumnValue value;

    /**
     * 列的类型
     */
    private ColumnType type;

    public RecordColumn(String name, ColumnValue value, ColumnType type) {
        setName(name);
        setValue(value);
        setType(type);
    }

    /**
     * 获取列名
     * @return 列名
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取列值
     * @return 列值，当该列类型为DELETE时，返回null
     */
    public ColumnValue getValue() {
        return value;
    }

    public void setValue(ColumnValue value) {
        this.value = value;
    }

    /**
     * 获取列的类型
     * @return 列的类型
     */
    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Name:");
        sb.append(getName());
        sb.append(",Value:");
        sb.append(getValue());
        sb.append(",ColumnType:");
        sb.append(getType());
        return sb.toString();
    }
}