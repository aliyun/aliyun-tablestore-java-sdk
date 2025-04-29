package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ParquetSchema {

    /**
     * Deliver the column name of the OTS table
     */
    private String columnName;

    /**
     * The column name to be delivered to OSS
     */
    private String ossColumnName;

    /**
     * Deliver OTS table column type
     */
    private DataType type;

    /**
     * Parquet encoding type
     */
    private OSSFileEncoding encode;

    /**
     * Parquet extension type
     */
    private String typeExtend;

    /**
     * Initialize the ParquetSchema instance
     */
    public ParquetSchema() {
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * Initialize the ParquetSchema instance
     *
     * @param columnName Column name in the OTS table
     * @param type       Data type of this column in the OTS table
     */
    public ParquetSchema(String columnName, DataType type) {
        setColumnName(columnName);
        setOssColumnName(columnName);
        setType(type);
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * Initialize a ParquetSchema instance
     *
     * @param columnName     Column name in the OTS table
     * @param ossColumnName  Column name delivered to OSS
     * @param type           Type of this column in the OTS table
     */
    public ParquetSchema(String columnName, String ossColumnName, DataType type) {
        setColumnName(columnName);
        setOssColumnName(ossColumnName);
        setType(type);
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * Get the column names in the OTS table
     *
     * @return Column names in the OTS table
     */
    public String getColumnName() {return columnName; }

    /**
     * Set the column name in the OTS table
     *
     * @param columnName The column name in the OTS table
     */
    public void setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "the columnName should not be null or empty");
        this.columnName = columnName;
    }

    /**
     * Get the column names delivered to OSS
     *
     * @return Column names on OSS
     */
    public String getOssColumnName() { return ossColumnName; }

    /**
     * Set the column name to be delivered to OSS
     *
     * @param ossColumnName Column name on OSS
     */
    public void setOssColumnName(String ossColumnName) {
        Preconditions.checkArgument(ossColumnName != null && !ossColumnName.isEmpty(), "the oss columnName should not be null or empty");
        this.ossColumnName = ossColumnName;
    }

    /**
     * Get the column type in the OTS table
     *
     * @return the column type in the OTS table
     */
    public DataType getType() { return type; }

    /**
     * Set the column type in the OTS table
     *
     * @param type The type of this column in the OTS table
     */
    public void setType(DataType type) {
        Preconditions.checkNotNull(type);
        this.type = type;
    }

    /**
     * Get the encoding type of this column
     *
     * @return the encoding type of this column
     */
    public OSSFileEncoding getEncode() { return encode; }

    /**
     * Set the encoding type for this column
     *
     * @param encode Set the encoding type for this column
     */
    public void setEncode(OSSFileEncoding encode) {
        Preconditions.checkNotNull(encode);
        this.encode = encode;
    }

    /**
     * Extended functionality, more Parquet types will be supported in the future.
     * Not supported yet.
     *
     * @return Parquet extended type
     */
    public String getTypeExtend() { return typeExtend; }

    /**
     * Get the parquet extended type
     * Not supported yet
     *
     * @param typeExtend parquet extended type
     */
    public void setTypeExtend(String typeExtend) {
        Preconditions.checkNotNull(typeExtend);
        this.typeExtend = typeExtend;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("columnName: ").append(columnName).append(", ossColumnName: ").append(ossColumnName)
                .append(", type: ").append(type).append(", encode: ").append(encode).append(", typeExtend: ").append(typeExtend)
                .append("}");
        return sb.toString();
    }
}
