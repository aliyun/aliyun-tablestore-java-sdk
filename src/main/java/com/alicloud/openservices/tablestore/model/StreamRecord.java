package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.List;

public class StreamRecord {

    public enum RecordType {
        /**
         * PUT类型
         * 如果对应行已存在，该Record需要覆盖原有数据。
         */
        PUT,

        /**
         * UPDATE类型
         * 如果对应行已存在，该Record是在原有数据上的更新。
         */
        UPDATE,

        /**
         * DELETE类型
         * 表明要删除对应的行。
         */
        DELETE
    }

    /**
     * Record的类型
     */
    private RecordType recordType;

    /**
     * 对应行的主键
     */
    private PrimaryKey primaryKey;

    /**
     * 对应行的时序信息
     */
    private RecordSequenceInfo sequenceInfo;

    /**
     * 该Record包含的属性列，为RecordColumn类型
     */
    private List<RecordColumn> columns;

    /**
     * 获取Record的类型
     * @return Record的类型
     */
    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    /**
     * 获取对应行的主键
     * @return 对应行的主键
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * 获取该行的时序信息
     * @return 该行的时序信息
     */
    public RecordSequenceInfo getSequenceInfo() {
        return sequenceInfo;
    }
    public void setSequenceInfo(RecordSequenceInfo sequenceInfo) {
        this.sequenceInfo = sequenceInfo;
    }

    /**
     * 获取该Record包含的属性列列表
     * @return 该Record包含的属性列列表
     */
    public List<RecordColumn> getColumns() {
        if (columns != null) {
            return columns;
        } else {
            return new ArrayList<RecordColumn>();
        }
    }

    public void setColumns(List<RecordColumn> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[RecordType:]");
        sb.append(this.recordType);
        sb.append("\n[RecordSequenceInfo:]");
        sb.append(this.sequenceInfo);
        sb.append("\n[PrimaryKey:]");
        sb.append(this.primaryKey);
        sb.append("\n[Columns:]");
        for (RecordColumn column : this.getColumns()) {
            sb.append("(");
            sb.append(column);
            sb.append(")");
        }
        return sb.toString();
    }

}
