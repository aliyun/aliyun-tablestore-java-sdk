package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OSSTaskConfig {

    /**
     * Delivery task OSS path prefix
     */
    private String ossPrefix;

    /**
     * OSS partition format
     */
    private TimeFormatter timeFormatter;

    /**
     * OSS Bucket
     */
    private String ossBucket;

    /**
     * Endpoint of the OSS service
     */

    private String ossEndpoint;

    /**
     * The StsRole with bucket write permissions assigned to the otsf service account
     */
    private String ossStsRole;

    /**
     * Event time column
     */
    private EventColumn eventTimeColumn;

    /**
     * The file format delivered to OSS, currently only Parquet is supported.
     */
    private OSSFileFormat format;

    /**
     * User-submitted task Schema
     */
    private List<ParquetSchema> parquetSchema = new ArrayList<ParquetSchema>();

    /**
     * Initialize the OSSTaskConfig instance
     */
    public OSSTaskConfig() {
        setTimeFormatter(TimeFormatter.YDMFormatter);
        //only support parquet
        setFormat(OSSFileFormat.Parquet);
    }

    /**
     * Get the OSS path prefix
     *
     * @return OSS path prefix
     */
    public String getOssPrefix() { return ossPrefix; }

    /**
     * Set the OSS path prefix
     *
     * @param ossPrefix The OSS path prefix
     */
    public void setOssPrefix(String ossPrefix) {
        Preconditions.checkArgument(ossPrefix != null && !ossPrefix.isEmpty(), "the ossPrefix should not be null or empty");
        this.ossPrefix = ossPrefix;
    }

    /**
     * Get the OSS partition format for delivery, currently only supports year-month-day format.
     *
     * @return OSS partition format
     */
    public TimeFormatter getTimeFormatter() { return timeFormatter; }

    /**
     * Set the OSS partition format for delivery
     *
     * @param timeFormatter The OSS partition format for delivery
     */
    public void setTimeFormatter(TimeFormatter timeFormatter) {
        Preconditions.checkNotNull(timeFormatter);
        this.timeFormatter = timeFormatter;
    }

    /**
     * Get OSS Bucket
     * @return OSS Bucket
     */
    public String getOssBucket() { return ossBucket; }

    /**
     * Set OSS Bucket
     * @param ossBucket OSS Bucket
     */
    public void setOssBucket(String ossBucket) {
        Preconditions.checkArgument(ossBucket != null && !ossBucket.isEmpty(), "the oss Bucket should not be null or empty");
        this.ossBucket = ossBucket;
    }

    /**
     * Get OSS endpoint
     * @return  OSS endpoint
     */
    public String getOssEndpoint() { return ossEndpoint; }

    /**
     * Set OSS endpoint
     * @param ossEndpoint OSS endpoint
     */
    public void setOssEndpoint(String ossEndpoint) {
        Preconditions.checkArgument(ossEndpoint != null && !ossEndpoint.isEmpty(), "the oss endpoint should not be null or empty");
        this.ossEndpoint = ossEndpoint;
    }

    /**
     * Get StsRole
     * @return StsRole
     */
    public String getOssStsRole() { return ossStsRole; }

    /**
     * Set StsRole
     * @param ossStsRole StsRole
     */
    public void setOssStsRole(String ossStsRole) {
        Preconditions.checkArgument(ossStsRole != null && !ossStsRole.isEmpty(), "the oss stsRole should not be null or empty");
        this.ossStsRole = ossStsRole;
    }

    /**
     * Get the event time column name
     * @return Event time column name
     */
    public EventColumn getEventTimeColumn() { return eventTimeColumn; }

    /**
     * Set the event time column name
     * @param eventTimeColumn The name of the event time column
     */
    public void setEventTimeColumn(EventColumn eventTimeColumn) {
        Preconditions.checkNotNull(eventTimeColumn);
        this.eventTimeColumn = eventTimeColumn;
    }

    /**
     * Get the file format delivered to OSS, currently only supports parquet format
     * @return File format on OSS
     */
    public OSSFileFormat getFormat() { return format; }

    /**
     * Set the file format delivered to OSS
     * @param format
     */
    public void setFormat(OSSFileFormat format) {
        Preconditions.checkNotNull(format);
        this.format = format;
    }

    /**
     * Get the delivery task schema
     * @return Delivery task schema
     */
    public List<ParquetSchema> getParquetSchemaList() { return Collections.unmodifiableList(parquetSchema); }

    /**
     * Add data delivery column
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