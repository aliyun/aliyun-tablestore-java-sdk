package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;

import java.util.ArrayList;
import java.util.List;

public class CreateTimeseriesTableRequest implements Request {

    public static class LastpointIndex {
        private String indexName;

        public LastpointIndex(String indexName) {
            this.indexName = indexName;
        }
        public String getIndexName() {
            return indexName;
        }
        public void setIndexName(String indexName) {
            this.indexName = indexName;
        }
    }

    /**
     * The structure information of the table.
     */
    private TimeseriesTableMeta timeseriesTableMeta;

    /**
     * Analyze storage information
     */
    private List<TimeseriesAnalyticalStore> analyticalStores = new ArrayList<TimeseriesAnalyticalStore>();

    /**
     * Whether to enable analytical storage
     */
    private boolean enableAnalyticalStore = true;

    private List<LastpointIndex> lastpointIndexes = new ArrayList<LastpointIndex>();

    /**
     * Initialize the CreateTimeseriesTableRequest instance.
     * <p>All table parameters are set to default values, currently only include timeseriestablemeta.
     * @param timeseriesTableMeta The structural information of the table.
     */
    public CreateTimeseriesTableRequest(TimeseriesTableMeta timeseriesTableMeta) {
        setTimeseriesTableMeta(timeseriesTableMeta);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TIMESERIES_TABLE;
    }

    /**
     * Get the table structure information.
     * @return Table structure information
     */
    public TimeseriesTableMeta getTimeseriesTableMeta() {
        return timeseriesTableMeta;
    }

    /**
     * Get the analytical storage information
     * @return Analytical storage information
     */
    public List<TimeseriesAnalyticalStore> getAnalyticalStores() {
        return analyticalStores;
    }

    /**
     * Get whether the analytical storage is enabled
     * @return Whether the analytical storage is enabled
     */
    public boolean isEnableAnalyticalStore() {
        return enableAnalyticalStore;
    }

    /**
     * Set the structure information of the table.
     * @param timeseriesTableMeta The structure information of the table
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        Preconditions.checkNotNull(timeseriesTableMeta, "timeseriesTableMeta should not be null.");
        this.timeseriesTableMeta = timeseriesTableMeta;
    }

    /**
     * Set the analytical storage information
     * @param analyticalStores Analytical storage information
     */
    public void setAnalyticalStores(List<TimeseriesAnalyticalStore> analyticalStores) {
        this.analyticalStores = analyticalStores;
    }

    /**
     * Set whether to enable analytical storage
     * @param enableAnalyticalStore whether to enable analytical storage
     */
    public void setEnableAnalyticalStore(boolean enableAnalyticalStore) {
        this.enableAnalyticalStore = enableAnalyticalStore;
    }

    public List<LastpointIndex> getLastpointIndexes() {
        return lastpointIndexes;
    }

    public void addLastpointIndex(LastpointIndex lastpointIndex) {
        this.lastpointIndexes.add(lastpointIndex);
    }
}
