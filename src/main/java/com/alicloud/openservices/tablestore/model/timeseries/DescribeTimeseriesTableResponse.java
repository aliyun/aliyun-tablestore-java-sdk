package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;

import java.util.ArrayList;
import java.util.List;

public class DescribeTimeseriesTableResponse extends Response {

    /**
     * 表的结构信息。
     */
    private TimeseriesTableMeta timeseriesTableMeta;

    /**
     * 分析存储信息。
     */
    private List<TimeseriesAnalyticalStore> analyticalStores = new ArrayList<TimeseriesAnalyticalStore>();

    private List<TimeseriesLastpointIndex> lastpointIndexes = new ArrayList<TimeseriesLastpointIndex>();

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

    /**
     * 获取分析存储信息。
     * @return 分析存储信息。
     */
    public List<TimeseriesAnalyticalStore> getAnalyticalStores() {
        return analyticalStores;
    }

    public List<TimeseriesLastpointIndex> getLastpointIndexes() {
        return lastpointIndexes;
    }

    /*
     * 内部接口。请勿使用。
     * @param timeseriesTableMeta 表的结构信息。
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        this.timeseriesTableMeta = timeseriesTableMeta;
    }

    /**
     * 内部接口。请勿使用。
     * @param analyticalStores 分析存储信息。
     */
    public void setAnalyticalStores(List<TimeseriesAnalyticalStore> analyticalStores) {
        this.analyticalStores = analyticalStores;
    }

    /*
     * 内部接口。请勿使用。
     * @param lastpointIndexes 最近点索引信息。
     */
    public void setLastpointIndexes(List<TimeseriesLastpointIndex> lastpointIndexes) {
        this.lastpointIndexes = lastpointIndexes;
    }
}
