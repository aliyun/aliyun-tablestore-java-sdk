package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;


public class UpdateTimeseriesTableRequest implements Request {

    /**
     * 表名信息和表的option信息
     */
    private String timeseriesTableName;
    private TimeseriesTableOptions timeseriesTableOptions;

    private TimeseriesMetaOptions timeseriesMetaOptions;

    /**
     * 初始化UpdateTimeseriesTableRequest实例。
     * @param timeseriesTableName 表名。
     * @param timeseriesTableOptions 表的options。
     */
    public UpdateTimeseriesTableRequest(String timeseriesTableName, TimeseriesTableOptions timeseriesTableOptions) {
        setTimeseriesTableName(timeseriesTableName);
        setTimeseriesTableOptions(timeseriesTableOptions);
    }

    public UpdateTimeseriesTableRequest(String timeseriesTableName) {
        setTimeseriesTableName(timeseriesTableName);
    }

    /**
     * 获取表的名称。
     * @return 表的名称。
     */
    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    /**
     * 获取表的options。
     * @return 表的options。
     */
    public TimeseriesTableOptions getTimeseriesTableOptions() {
        return timeseriesTableOptions;
    }

    /**
     * 设置表的名称。
     * @param timeseriesTableName 表的名称。
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");
        this.timeseriesTableName = timeseriesTableName;
    }

    /**
     * 获取时间线元数据相关配置
     * @return
     */
    public TimeseriesMetaOptions getTimeseriesMetaOptions() {
        return timeseriesMetaOptions;
    }

    /**
     * 设置时间线元数据相关配置
     * @param timeseriesMetaOptions
     */
    public void setTimeseriesMetaOptions(TimeseriesMetaOptions timeseriesMetaOptions) {
        this.timeseriesMetaOptions = timeseriesMetaOptions;
    }

    /**
     * 设置表的options。
     * @param timeseriesTableOptions 表的options。
     */
    public void setTimeseriesTableOptions(TimeseriesTableOptions timeseriesTableOptions) {
        Preconditions.checkNotNull(timeseriesTableOptions, "TimeseriesTableOptions should not be null.");
        this.timeseriesTableOptions = timeseriesTableOptions;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_TIMESERIES_TABLE;
    }

}
