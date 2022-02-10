package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ParquetSchema {

    /**
     * 投递OTS表列名
     */
    private String columnName;

    /**
     * 投递到OSS上的列名
     */
    private String ossColumnName;

    /**
     * 投递OTS表列类型
     */
    private DataType type;

    /**
     *parquet编码类型
     */
    private OSSFileEncoding encode;

    /**
     *parquet扩展类型
     */
    private String typeExtend;

    /**
     * 初始化ParquetSchema实例
     */
    public ParquetSchema() {
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * 初始化ParquetSchema实例
     *
     * @param columnName ots表中列名
     * @param type       ots表中该列类型
     */
    public ParquetSchema(String columnName, DataType type) {
        setColumnName(columnName);
        setOssColumnName(columnName);
        setType(type);
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * 初始化ParquetSchema实例
     *
     * @param columnName     OTS表中列名
     * @param ossColumnName  投递到OSS列名
     * @param type           OTS表中该列的类型
     */
    public ParquetSchema(String columnName, String ossColumnName, DataType type) {
        setColumnName(columnName);
        setOssColumnName(ossColumnName);
        setType(type);
        setEncode(OSSFileEncoding.PLAIN);
    }

    /**
     * 获取OTS表中列名
     *
     * @return OTS表中列名
     */
    public String getColumnName() {return columnName; }

    /**
     * 设置OTS表中列名
     *
     * @param columnName OTS表中列名
     */
    public void setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "the columnName should not be null or empty");
        this.columnName = columnName;
    }

    /**
     * 获取投递到OSS上的列名
     *
     * @return OSS上列名
     */
    public String getOssColumnName() { return ossColumnName; }

    /**
     * 设置投递到OSS上的列名
     *
     * @param ossColumnName OSS上列名
     */
    public void setOssColumnName(String ossColumnName) {
        Preconditions.checkArgument(ossColumnName != null && !ossColumnName.isEmpty(), "the oss columnName should not be null or empty");
        this.ossColumnName = ossColumnName;
    }

    /**
     * 获取OTS表中列类型
     *
     * @return OTS表中该列类型
     */
    public DataType getType() { return type; }

    /**
     * 设置OTS表中列类型
     *
     * @param type OTS表中该列类型
     */
    public void setType(DataType type) {
        Preconditions.checkNotNull(type);
        this.type = type;
    }

    /**
     * 获取该列的编码类型
     *
     * @return 该列编码类型
     */
    public OSSFileEncoding getEncode() { return encode; }

    /**
     *设置该列编码类型
     *
     * @param encode 设置该列编码类型
     */
    public void setEncode(OSSFileEncoding encode) {
        Preconditions.checkNotNull(encode);
        this.encode = encode;
    }

    /**
     * 扩展功能，未来会支持更多parquet类型
     * 暂不支持
     *
     * @return parquet扩展类型
     */
    public String getTypeExtend() { return typeExtend; }

    /**
     * 获取parquet扩展类型
     * 暂不支持
     *
     * @param typeExtend parquet扩展类型
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
