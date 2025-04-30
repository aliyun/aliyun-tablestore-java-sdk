package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;

import java.util.ArrayList;
import java.util.List;

public class DescribeTimeseriesTableResponse extends Response {

    /**
     * The structural information of the table.
     */
    private TimeseriesTableMeta timeseriesTableMeta;

    /**
     * Analyze storage information.
     */
    private List<TimeseriesAnalyticalStore> analyticalStores = new ArrayList<TimeseriesAnalyticalStore>();

    private List<TimeseriesLastpointIndex> lastpointIndexes = new ArrayList<TimeseriesLastpointIndex>();

    public DescribeTimeseriesTableResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the tablemeta of the table.
     * @return The tablemeta of the table.
     */
    public TimeseriesTableMeta getTimeseriesTableMeta() {
        return timeseriesTableMeta;
    }

    /**
     * Get the analytical store information.
     * @return Analytical store information.
     */
    public List<TimeseriesAnalyticalStore> getAnalyticalStores() {
        return analyticalStores;
    }

    public List<TimeseriesLastpointIndex> getLastpointIndexes() {
        return lastpointIndexes;
    }

    /*
     * Internal interface. Do not use.
     * @param timeseriesTableMeta The structural information of the table.
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        this.timeseriesTableMeta = timeseriesTableMeta;
    }

    /**
     * Internal interface. Do not use.
     * @param analyticalStores Analytical store information.
     */
    public void setAnalyticalStores(List<TimeseriesAnalyticalStore> analyticalStores) {
        this.analyticalStores = analyticalStores;
    }

    /*
     * Internal interface. Do not use.
     * @param lastpointIndexes Information of the nearest point index.
     */
    public void setLastpointIndexes(List<TimeseriesLastpointIndex> lastpointIndexes) {
        this.lastpointIndexes = lastpointIndexes;
    }
}
