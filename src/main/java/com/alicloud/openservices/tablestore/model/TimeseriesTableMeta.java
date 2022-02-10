package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 表的结构信息，包含表的名称以及表的配置信息。
 */
public class TimeseriesTableMeta implements Jsonizable {

    /**
     * 表的名称。
     */
    private String timeseriesTableName;

    /**
     * 表的配置项, 目前只包括TTL。
     */
    private TimeseriesTableOptions timeseriesTableOptions;

    /**
     * 表的状态。
     */
    private String status;

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
        Preconditions.checkNotNull(timeseriesTableOptions, "TimeseriesTableOptionsEx should not be null.");
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
