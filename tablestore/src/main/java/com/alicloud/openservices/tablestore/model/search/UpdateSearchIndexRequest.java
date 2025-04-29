package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UpdateSearchIndexRequest implements Request {
    private String tableName;

    private String indexName;

    private String switchIndexName;

    private List<QueryFlowWeight> queryFlowWeight;

    /**
     * <p>The TTL time for index data, in seconds.</p>
     */
    private Integer timeToLive;

    /**
     * <p>For dynamically adding new fields to the index, this operation is more lightweight compared to dynamically modifying the index schema, but it can only add fields, and the types of the added fields have certain restrictions.</p>
     */
    private List<FieldSchema> addedFieldSchemas;

    public UpdateSearchIndexRequest(String tableName, String indexName) {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    /**
     * Index switch request constructor
     * <br> In the scenario of dynamically modifying the schema, when the index rebuilding synchronization catches up with the source index and the A/B test has been fully verified, the "index switch" can be performed. After the switch, all query traffic will be directed to the index of the new schema.
     * @param tableName Table name
     * @param indexName Index name
     * @param switchIndexName The index name to be switched 
     */
    public UpdateSearchIndexRequest(String tableName, String indexName, String switchIndexName) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.switchIndexName = switchIndexName;
    }

    /**
     * Set the query traffic weight
     * @param tableName Table name
     * @param indexName Index name
     * @param queryFlowWeight Query traffic weight list. Sets the traffic distribution weight for querying the "source index" and "rebuilding index", the list length is 2.
     *                        e.g., setting the weights to [(index1, 20), (index2, 80)] means that 20% of the query traffic will go to index1, and 80% of the query traffic will go to index2.
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
     * <p>TTL time for index data</p>
     * @param days ttl, parameter unit is day
     */
    public UpdateSearchIndexRequest setTimeToLiveInDays(int days) {
        this.setTimeToLive(days, TimeUnit.DAYS);
        return this;
    }

    /**
     * <p>TTL time for index data.</p>
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

    public List<FieldSchema> getAddedFieldSchemas() {
        return addedFieldSchemas;
    }

    public UpdateSearchIndexRequest setAddedFieldSchemas(List<FieldSchema> addedFieldSchemas) {
        if (this.addedFieldSchemas == null) {
            this.addedFieldSchemas = new ArrayList<>();
        }
        this.addedFieldSchemas.addAll(addedFieldSchemas);
        return this;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_SEARCH_INDEX;
    }
}
