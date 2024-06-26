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
     * 表的结构信息。
     */
    private TimeseriesTableMeta timeseriesTableMeta;

    /**
     * 分析存储信息
     */
    private List<TimeseriesAnalyticalStore> analyticalStores = new ArrayList<TimeseriesAnalyticalStore>();

    /**
     * 是否开启分析存储
     */
    private boolean enableAnalyticalStore = true;

    private List<LastpointIndex> lastpointIndexes = new ArrayList<LastpointIndex>();

    /**
     * 初始化CreateTimeseriesTableRequest实例。
     * <p>表的各项均为默认值，目前只包含timeseriestablemeta
     * @param timeseriesTableMeta 表的结构信息。
     */
    public CreateTimeseriesTableRequest(TimeseriesTableMeta timeseriesTableMeta) {
        setTimeseriesTableMeta(timeseriesTableMeta);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TIMESERIES_TABLE;
    }

    /**
     * 获取表的结构信息。
     * @return 表的结构信息
     */
    public TimeseriesTableMeta getTimeseriesTableMeta() {
        return timeseriesTableMeta;
    }

    /**
     * 获取分析存储信息
     * @return 分析存储信息
     */
    public List<TimeseriesAnalyticalStore> getAnalyticalStores() {
        return analyticalStores;
    }

    /**
     * 获取是否开启分析存储
     * @return 是否开启分析存储
     */
    public boolean isEnableAnalyticalStore() {
        return enableAnalyticalStore;
    }

    /**
     * 设置表的结构信息。
     * @param timeseriesTableMeta 表的结构信息
     */
    public void setTimeseriesTableMeta(TimeseriesTableMeta timeseriesTableMeta) {
        Preconditions.checkNotNull(timeseriesTableMeta, "timeseriesTableMeta should not be null.");
        this.timeseriesTableMeta = timeseriesTableMeta;
    }

    /**
     * 设置分析存储信息
     * @param analyticalStores 分析存储信息
     */
    public void setAnalyticalStores(List<TimeseriesAnalyticalStore> analyticalStores) {
        this.analyticalStores = analyticalStores;
    }

    /**
     * 设置是否开启分析存储
     * @param enableAnalyticalStore 是否开启分析存储
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
