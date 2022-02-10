package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;


public class DescribeTimeseriesTableRequest implements Request {


    /**
     * 表名信息。
     */
    private String timeseriesTableName;

    /**
     * 初始化DeleteTimeseriesTableRequest实例。
     * @param timeseriesTableName 表名。
     */
    public DescribeTimeseriesTableRequest(String timeseriesTableName) {
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
     * 设置表的名称。
     * @param timeseriesTableName 表的名称。
     */
    public void setTimeseriesTableName(String timeseriesTableName) {
        Preconditions.checkArgument(
                timeseriesTableName != null && !timeseriesTableName.isEmpty(),
                "The name of table should not be null or empty.");

        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_TIMESERIES_TABLE;
    }

}
