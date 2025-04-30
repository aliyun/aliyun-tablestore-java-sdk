package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.List;

public class StreamRecord {

    public enum RecordType {
        /**
         * PUT type
         * If the corresponding row already exists, this Record should overwrite the existing data.
         */
        PUT,

        /**
         * UPDATE type
         * If the corresponding row already exists, this Record is an update based on the existing data.
         */
        UPDATE,

        /**
         * DELETE type
         * Indicates that the corresponding row needs to be deleted.
         */
        DELETE
    }

    /**
     * The type of Record
     */
    private RecordType recordType;

    /**
     * The primary key corresponding to the row
     */
    private PrimaryKey primaryKey;

    /**
     * Corresponds to the timing information of the row
     */
    private RecordSequenceInfo sequenceInfo;

    /**
     * The property columns contained in this Record, of type RecordColumn.
     */
    private List<RecordColumn> columns;

    /**
    * The original attribute columns contained in this Record, of type RecordColumn
    */
    private List<RecordColumn> originColumns;

    /**
     * Get the type of Record
     * @return the type of Record
     */
    public RecordType getRecordType() {
        return recordType;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    /**
     * Get the primary key of the corresponding row
     * @return the primary key of the corresponding row
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * Get the timestamp information of this row
     * @return The timestamp information of this row
     */
    public RecordSequenceInfo getSequenceInfo() {
        return sequenceInfo;
    }
    public void setSequenceInfo(RecordSequenceInfo sequenceInfo) {
        this.sequenceInfo = sequenceInfo;
    }

    /**
     * Get the list of attribute columns contained in this Record
     * @return the list of attribute columns contained in this Record
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

    /**
     * Get the list of original attribute columns contained in this Record
     * @return the list of original attribute columns contained in this Record
     */
    public List<RecordColumn> getOriginColumns() {
        if (originColumns != null) {
            return originColumns;
        } else {
            return new ArrayList<RecordColumn>();
        }
    }

    public void setOriginColumns(List<RecordColumn> originColumns) {
        this.originColumns = originColumns;
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
        sb.append("\n[originColumns:]");
        for (RecordColumn originColumn : this.getOriginColumns()) {
            sb.append("(");
            sb.append(originColumn);
            sb.append(")");
        }
        return sb.toString();
    }

}
