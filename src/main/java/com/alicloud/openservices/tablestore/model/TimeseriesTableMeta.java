package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * 表的结构信息，包含表的名称以及表的配置信息。
 */
public class TimeseriesTableMeta implements Jsonizable {

    /**
     * 表的名称。
     */
    private String timeseriesTableName;

    /**
     * 表的配置项, 包括数据的TTL等。
     */
    private TimeseriesTableOptions timeseriesTableOptions;

    /**
     * 时间线元数据相关配置项，包括元数据的TTL等。
     */
    private TimeseriesMetaOptions timeseriesMetaOptions;

    /**
     * 表的状态。
     */
    private String status;

    /**
     * 自定义主键列。
     */
    private final List<String> timeseriesKeys = new ArrayList<String>();

    /**
     * 扩展主键列。
     */
    private final List<PrimaryKeySchema> fieldPrimaryKeys = new ArrayList<PrimaryKeySchema>();

    /**
     * 创建一个新的给定表名的<code>TableMeta</code>实例。
     *
     * @param timeseriesTableName 表名。
     */
    public TimeseriesTableMeta(String timeseriesTableName) {
        this(timeseriesTableName, new TimeseriesTableOptions());
    }

    public TimeseriesTableMeta(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
    }

    /* 内部使用 */
    public TimeseriesTableMeta(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions, String status) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
        setStatus(status);
    }

    /**
     * 设置表的名称。
     *
     * @param timeseriesTableName 表的名称。
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(timeseriesTableName != null && !timeseriesTableName.isEmpty(), "The name of table should not be null or empty.");

        this.timeseriesTableName = timeseriesTableName;
    }

    /**
     * 返回表的名称。
     *
     * @return 表的名称。
     */
    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    /**
     * 设置表的配置参数。
     *
     * @param timeseriesTableOptions 表的配置。
     */
    public void setTimeseriesTableOptions(TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkNotNull(timeseriesTableOptions, "TimeseriesTableOptions should not be null.");
        this.timeseriesTableOptions = timeseriesTableOptions;
    }

    /**
     * 返回表的配置。
     *
     * @return 表的配置。
     */
    public TimeseriesTableOptions getTimeseriesTableOptions() {
        return timeseriesTableOptions;
    }

    public TimeseriesMetaOptions getTimeseriesMetaOptions() {
        return timeseriesMetaOptions;
    }

    public void setTimeseriesMetaOptions(TimeseriesMetaOptions timeseriesMetaOptions) {
        Preconditions.checkNotNull(timeseriesMetaOptions, "TimeseriesMetaOptions should not be null.");
        this.timeseriesMetaOptions = timeseriesMetaOptions;
    }

    /**
     * 设置表的配置参数。
     *
     * @param status 表的配置。
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * 返回表的配置。
     *
     * @return 表的配置。
     */
    public String getStatus() {
        return status;
    }

    public void addTimeseriesKey(String primaryKey) {
        timeseriesKeys.add(primaryKey);
    }

    public List<String> getTimeseriesKeys() {
        return timeseriesKeys;
    }

    public void addFieldPrimaryKey(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key field should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key field should not be null.");
        fieldPrimaryKeys.add(new PrimaryKeySchema(name, type));
    }

    /**
     * 返回扩展主键列。
     *
     * @return 扩展主键列。
     */
    public List<PrimaryKeySchema> getFieldPrimaryKeys() {
        return fieldPrimaryKeys;
    }

    @Override
    public String toString() {
        String s = "TimeseriesTableName: " + timeseriesTableName;
        return s;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"TimeseriesTableName\": \"");
        sb.append(timeseriesTableName);
        sb.append('\"');

        sb.append("}");
    }
}
