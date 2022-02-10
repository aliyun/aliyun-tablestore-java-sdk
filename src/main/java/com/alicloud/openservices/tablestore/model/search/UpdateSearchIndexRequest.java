package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class UpdateSearchIndexRequest implements Request {
    private String tableName;

    private String indexName;

    private String switchIndexName;

    private List<QueryFlowWeight> queryFlowWeight;

    /**
     * <p>索引数据的TTL时间，单位为秒。</p>
     */
    private Integer timeToLive;

    public UpdateSearchIndexRequest(String tableName, String indexName) {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    /**
     * 索引交换请求 构造方法
     * <br> 在动态修改schema的场景下，当重建索引同步追上源索引，且AB test充分验证后，才能"交换索引"。交换后，所有的查询流量都会打到新schema的索引上。
     * @param tableName 表名
     * @param indexName 索引名
     * @param switchIndexName the index to be switched 被交换索引名
     */
    public UpdateSearchIndexRequest(String tableName, String indexName, String switchIndexName) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.switchIndexName = switchIndexName;
    }

    /**
     * 设置查询流量权重
     * @param tableName 表名
     * @param indexName 索引名
     * @param queryFlowWeight 查询流量权重列表。设置"源索引"和"重建索引"被查询时的流量分配权重，列表长度为2。
     *                        e.g. 设置权重为[(index1, 20), (index2, 80)]，表示 20%的查询流量会打到index1，80%的查询流量会打到index2
     */
    public UpdateSearchIndexRequest(String tableName, String indexName, List<QueryFlowWeight> queryFlowWeight) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.queryFlowWeight = queryFlowWeight;
    }

    public String getTableName() {
        return tableName;
    }

    public UpdateSearchIndexRequest setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getIndexName() {
        return indexName;
    }

    public UpdateSearchIndexRequest setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public String getSwitchIndexName() {
        return switchIndexName;
    }

    public UpdateSearchIndexRequest setSwitchIndexName(String switchIndexName) {
        this.switchIndexName = switchIndexName;
        return this;
    }

    public List<QueryFlowWeight> getQueryFlowWeight() {
        return queryFlowWeight;
    }

    public UpdateSearchIndexRequest setQueryFlowWeight(List<QueryFlowWeight> queryFlowWeight) {
        this.queryFlowWeight = queryFlowWeight;
        return this;
    }

    public Integer getTimeToLive() {
        return timeToLive;
    }

    /**
     * <p>索引数据的TTL时间</p>
     * @param days ttl, 参数单位是天
     */
    public UpdateSearchIndexRequest setTimeToLiveInDays(int days) {
        this.setTimeToLive(days, TimeUnit.DAYS);
        return this;
    }

    /**
     * <p>索引数据的TTL时间。</p>
     */
    public UpdateSearchIndexRequest setTimeToLive(int timeToLive, TimeUnit timeUnit) {
        Preconditions.checkArgument(timeToLive > 0 || timeToLive == -1,
                "The value of timeToLive can be -1 or any positive value.");
        if (timeToLive == -1) {
            this.timeToLive = -1;
        } else {
            long seconds = timeUnit.toSeconds(timeToLive);
            this.timeToLive = NumberUtils.longToInt(seconds);
        }
        return this;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_SEARCH_INDEX;
    }
}
