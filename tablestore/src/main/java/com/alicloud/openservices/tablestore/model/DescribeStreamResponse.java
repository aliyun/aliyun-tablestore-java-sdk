package com.alicloud.openservices.tablestore.model;

import java.util.List;

public class DescribeStreamResponse extends Response {

    /**
     * The Id of the Stream
     */
    private String streamId;

    /**
     * Expiration time of the Stream
     */
    private int expirationTime;

    /**
     * The name of the table it belongs to
     */
    private String tableName;

    /**
     * The creation time of the Stream
     */
    private long creationTime;

    /**
     * The state of the Stream
     */
    private StreamStatus status;

    /**
     * List of shards
     */
    private List<StreamShard> shards;

    /**
     * Used to specify the left boundary (InclusiveStartShardId) of the Shard list to be returned in the next request.
     */
    private String nextShardId;

    private boolean isTimeseriesDataTable;

    public DescribeStreamResponse() {

    }

    public DescribeStreamResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the Id of the Stream
     * @return Id of the Stream
     */
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * Get the name of the table
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the list of Shards
     * @return List of Shards
     */
    public List<StreamShard> getShards() {
        return shards;
    }

    public void setShards(List<StreamShard> shards) {
        this.shards = shards;
    }

    /**
     * Get the NextShardId parameter
     * @return NextShardId parameter
     */
    public String getNextShardId() {
        return nextShardId;
    }

    public void setNextShardId(String nextShardId) {
        this.nextShardId = nextShardId;
    }

    /**
     * Get the status of the Stream
     * @return Status of the Stream
     */
    public StreamStatus getStatus() {
        return status;
    }

    public void setStatus(StreamStatus status) {
        this.status = status;
    }

    /**
     * Get the expiration time of the Stream, in hours.
     * @return Expiration time of the Stream
     */
    public int getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * Get the creation time of the Stream, in microseconds.
     * @return The creation time of the Stream, in microseconds.
     */
    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public boolean isTimeseriesDataTable() {
        return isTimeseriesDataTable;
    }

    public void setTimeseriesDataTable(boolean timeseriesDataTable) {
        isTimeseriesDataTable = timeseriesDataTable;
    }
}

