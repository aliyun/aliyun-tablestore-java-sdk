package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;

public class DescribeTimeseriesTableResponse extends Response {

    /**
     * 表的结构信息。
     */
    private TimeseriesTableMeta timeseriesTableMeta;


    public DescribeTimeseriesTableResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取表的tablemeta。
     * @return 表的tablemeta。
     */
    public TimeseriesTableMeta getTimeseriesTableMeta() {
        return timeseriesTableMeta;
    }

    /*
     * 内部接口。请勿使用。
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        this.timeseriesTableMeta = timeseriesTableMeta;
    }
}
