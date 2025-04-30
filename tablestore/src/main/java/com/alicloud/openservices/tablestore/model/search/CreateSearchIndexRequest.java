package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.concurrent.TimeUnit;

public class CreateSearchIndexRequest implements Request {

    /**
     * The table name in TableStore
     */
    private String tableName;
    /**
     * The name of the SearchIndex.
     */
    private String indexName;

    /**
     * Schema structure of SearchIndex
     */
    private IndexSchema indexSchema;

    /**
     * In general, this field does not need to be set.
     * Only in the scenario of dynamically modifying the multi-index schema, use the setter method to set this field as the name of the source index for rebuilding the index.
     */
    private String sourceIndexName;

    /**
     * <p>The TTL time for index data, in seconds.</p>
     * <p>After the table is created, this configuration item can be dynamically changed by calling {@link UpdateSearchIndexRequest}.</p>
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
     * Get the TTL time, in seconds.
     * @return TTL time
     */
    public Integer getTimeToLive() {
        return timeToLive;
    }

    /**
     * <p>TTL time for index data</p>
     * <p>After the table is created, this configuration item can be dynamically changed by calling {@link UpdateSearchIndexRequest}.</p>
     *
     * @param days ttl, parameter unit is in days
     */
    public CreateSearchIndexRequest setTimeToLiveInDays(int days) {
        this.setTimeToLive(days, TimeUnit.DAYS);
        return this;
    }

    /**
     * <p>The TTL time for index data.</p>
     * <p>After the table is created, this configuration item can be dynamically changed by calling {@link UpdateSearchIndexRequest}.</p>
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
