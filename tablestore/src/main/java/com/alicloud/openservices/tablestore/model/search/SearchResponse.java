package com.alicloud.openservices.tablestore.model.search;

import java.util.List;

import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByResults;
import com.google.gson.GsonBuilder;

/**
 * The return result of SearchIndex
 */
public class SearchResponse extends Response {

    /**
     * Query based on the input Query statement, return the total number of hits from the SearchIndex engine.
     * <p>Note: This is the actual number of items queried, not the specific number of rows returned in this Response. The number of rows can be controlled by other parameters for operations similar to pagination.</p>
     */
    private long totalCount;

    /**
     * The specific return result list of the Query operation
     */
    private List<Row> rows;

    /**
     * Search query related results, encapsulating Row data and Highlight high-light result data.
     */
    private List<SearchHit> searchHits;
    
    /**
     * Whether the query was successful
     */
    private boolean isAllSuccess;

    private byte[] nextToken;

    private long bodyBytes;

    private ConsumedCapacity consumed;

    private ConsumedCapacity reservedConsumed;

    private AggregationResults aggregationResults;

    private GroupByResults groupByResults;

    public SearchResponse(Response meta) {
        super(meta);
    }

    public AggregationResults getAggregationResults() {
        return aggregationResults;
    }

    public SearchResponse setAggregationResults(
        AggregationResults aggregationResults) {
        this.aggregationResults = aggregationResults;
        return this;
    }

    public GroupByResults getGroupByResults() {
        return groupByResults;
    }

    public SearchResponse setGroupByResults(
        GroupByResults groupByResults) {
        this.groupByResults = groupByResults;
        return this;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public List<SearchHit> getSearchHits() {
        return searchHits;
    }

    public void setSearchHits(List<SearchHit> searchHits) {
        this.searchHits = searchHits;
    }

    public boolean isAllSuccess() {
        return isAllSuccess;
    }

    public void setAllSuccess(boolean allSuccess) {
        isAllSuccess = allSuccess;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }

    public long getBodyBytes() {
        return bodyBytes;
    }
    public void setBodyBytes(long bodyBytes) {
        this.bodyBytes = bodyBytes;
    }

    public void setConsumed(ConsumedCapacity consumed) {
        this.consumed = consumed;
    }

    public ConsumedCapacity getConsumed() {
        return this.consumed;
    }

    public void setReservedConsumed(ConsumedCapacity reservedConsumed) {
        this.reservedConsumed = reservedConsumed;
    }

    public ConsumedCapacity getReservedConsumed() {
        return this.reservedConsumed;
    }

    public String getResponseInfo(boolean prettyFormat) {
        GsonBuilder builder = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization();
        if (prettyFormat) {
            return builder.setPrettyPrinting().create().toJson(this);
        } else {
            return builder.create().toJson(this);
        }
    }

    public void printResponseInfo() {
        System.out.println(getResponseInfo(true));
    }
}
