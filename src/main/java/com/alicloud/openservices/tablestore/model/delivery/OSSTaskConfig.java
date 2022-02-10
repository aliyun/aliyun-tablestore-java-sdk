package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OSSTaskConfig {

    /**
     * 投递任务OSS路径前缀
     */
    private String ossPrefix;

    /**
     * OSS分区格式
     */
    private TimeFormatter timeFormatter;

    /**
     * OSS Bucket
     */
    private String ossBucket;

    /**
     * OSS服务的endpoint
     */

    private String ossEndpoint;

    /**
     * 赋予otsf服务账号的且有bucket写入权限的StsRole
     */
    private String ossStsRole;

    /**
     * 事件时间列
     */
    private EventColumn eventTimeColumn;

    /**
     * 投递到OSS文件格式，目前只支持Parquet
     */
    private OSSFileFormat format;

    /**
     * 用户投递任务Schema
     */
    private List<ParquetSchema> parquetSchema = new ArrayList<ParquetSchema>();

    /**
     * 初始化OSSTaskConfig实例
     */
    public OSSTaskConfig() {
        setTimeFormatter(TimeFormatter.YDMFormatter);
        //only support parquet
        setFormat(OSSFileFormat.Parquet);
    }

    /**
     * 获取OSS路径前缀
     *
     * @return OSS路径前缀
     */
    public String getOssPrefix() { return ossPrefix; }

    /**
     * 设置OSS路径前缀
     *
     * @param ossPrefix OSS路径前缀
     */
    public void setOssPrefix(String ossPrefix) {
        Preconditions.checkArgument(ossPrefix != null && !ossPrefix.isEmpty(), "the ossPrefix should not be null or empty");
        this.ossPrefix = ossPrefix;
    }

    /**
     * 获取投递OSS分区格式，目前仅支持年月日格式
     *
     * @return OSS分区格式
     */
    public TimeFormatter getTimeFormatter() { return timeFormatter; }

    /**
     * 设置投递OSS分区格式
     *
     * @param timeFormatter 投递OSS分区格式
     */
    public void setTimeFormatter(TimeFormatter timeFormatter) {
        Preconditions.checkNotNull(timeFormatter);
        this.timeFormatter = timeFormatter;
    }

    /**
     * 获取OSS Bucket
     * @return OSS Bucket
     */
    public String getOssBucket() { return ossBucket; }

    /**
     * 设置OSS Bucket
     * @param ossBucket OSS Bucket
     */
    public void setOssBucket(String ossBucket) {
        Preconditions.checkArgument(ossBucket != null && !ossBucket.isEmpty(), "the oss Bucket should not be null or empty");
        this.ossBucket = ossBucket;
    }

    /**
     * 获取OSS endpoint
     * @return  OSS endpoint
     */
    public String getOssEndpoint() { return ossEndpoint; }

    /**
     * 设置OSS endpoint
     * @param ossEndpoint OSS endpoint
     */
    public void setOssEndpoint(String ossEndpoint) {
        Preconditions.checkArgument(ossEndpoint != null && !ossEndpoint.isEmpty(), "the oss endpoint should not be null or empty");
        this.ossEndpoint = ossEndpoint;
    }

    /**
     * 获取StsRole
     * @return StsRole
     */
    public String getOssStsRole() { return ossStsRole; }

    /**
     * 设置StsRole
     * @param ossStsRole StsRole
     */
    public void setOssStsRole(String ossStsRole) {
        Preconditions.checkArgument(ossStsRole != null && !ossStsRole.isEmpty(), "the oss stsRole should not be null or empty");
        this.ossStsRole = ossStsRole;
    }

    /**
     * 获取事件时间列名
     * @return 事件时间列名
     */
    public EventColumn getEventTimeColumn() { return eventTimeColumn; }

    /**
     * 设置事件时间列名
     * @param eventTimeColumn 事件时间列名
     */
    public void setEventTimeColumn(EventColumn eventTimeColumn) {
        Preconditions.checkNotNull(eventTimeColumn);
        this.eventTimeColumn = eventTimeColumn;
    }

    /**
     * 获取投递OSS上文件格式，目前仅支持parquet格式
     * @return OSS上文件格式
     */
    public OSSFileFormat getFormat() { return format; }

    /**
     * 设置投递OSS上文件格式
     * @param format
     */
    public void setFormat(OSSFileFormat format) {
        Preconditions.checkNotNull(format);
        this.format = format;
    }

    /**
     * 获取投递任务Schema
     * @return 投递任务Schema
     */
    public List<ParquetSchema> getParquetSchemaList() { return Collections.unmodifiableList(parquetSchema); }

    /**
     * 添加数据投递列
     * @param parquetSchema
     */
    public void addParquetSchema(ParquetSchema parquetSchema) {
        Preconditions.checkNotNull(parquetSchema);
        this.parquetSchema.add(parquetSchema);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{").append("ossPrefix: ").append(ossPrefix).append(", timeFormatter: ")
                .append(timeFormatter).append(", ossBucket: ").append(ossBucket).append(", ossEndpoint: ").append(ossEndpoint)
                .append(", ossStsRole: ").append(ossStsRole).append(", eventTimeColumn: ").append(eventTimeColumn)
                .append(", format: ").append(format.name()).append(", parquetSchema: ").append(parquetSchema);
        return sb.toString();
    }
}