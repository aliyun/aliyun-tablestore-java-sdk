package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.concurrent.TimeUnit;

public class CreateSearchIndexRequest implements Request {

    /**
     * TableStore中的表名称
     */
    private String tableName;
    /**
     * SearchIndex的名称。
     */
    private String indexName;

    /**
     * SearchIndex的schema结构
     */
    private IndexSchema indexSchema;

    /**
     * 一般情况下，不需要设置本字段。
     * 仅在动态修改多元索引schema的场景下，通过setter方法进行设置本字段，作为重建索引的源索引名字。
     */
    private String sourceIndexName;

    /**
     * <p>索引数据的TTL时间，单位为秒。</p>
     * <p>在表创建后，该配置项可通过调用{@link UpdateSearchIndexRequest}动态更改。</p>
     */
    private Integer timeToLive;

    public CreateSearchIndexRequest() {
    }

    public CreateSearchIndexRequest(String tableName, String indexName) {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexSchema getIndexSchema() {
        return indexSchema;
    }

    public void setIndexSchema(IndexSchema indexSchema) {
        this.indexSchema = indexSchema;
    }

    public String getSourceIndexName() {
        return sourceIndexName;
    }

    public void setSourceIndexName(String sourceIndexName) {
        this.sourceIndexName = sourceIndexName;
    }

    /**
     * 获取TTL时间，单位为秒。
     * @return TTL时间
     */
    public Integer getTimeToLive() {
        return timeToLive;
    }

    /**
     * <p>索引数据的TTL时间</p>
     * <p>在表创建后，该配置项可通过调用{@link UpdateSearchIndexRequest}动态更改。</p>
     *
     * @param days ttl, 参数单位是天
     */
    public CreateSearchIndexRequest setTimeToLiveInDays(int days) {
        this.setTimeToLive(days, TimeUnit.DAYS);
        return this;
    }

    /**
     * <p>索引数据的TTL时间。</p>
     * <p>在表创建后，该配置项可通过调用{@link UpdateSearchIndexRequest}动态更改。</p>
     */
    public CreateSearchIndexRequest setTimeToLive(int timeToLive, TimeUnit timeUnit) {
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
        return OperationNames.OP_CREATE_SEARCH_INDEX;
    }
}
