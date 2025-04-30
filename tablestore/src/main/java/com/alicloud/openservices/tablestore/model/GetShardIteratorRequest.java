package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetShardIteratorRequest implements Request {

    /**
     * Stream's Id
     */
    private String streamId;

    /**
     * The Id of the Shard
     */
    private String shardId;

    /**
     * Get the ShardIterator according to the time, with time unit in us.
     */
    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * For paginated retrieval
     */
    private OptionalValue<String> token = new OptionalValue<String>("Token");

    public GetShardIteratorRequest(String streamId, String shardId) {
        setStreamId(streamId);
        setShardId(shardId);
    }

    /**
     * Get the StreamId parameter
     * @return StreamId
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Set the StreamId parameter
     * @param streamId
     */
    public void setStreamId(String streamId) {
        Preconditions.checkArgument(streamId != null && !streamId.isEmpty(), "The streamId should not be null or empty.");
        this.streamId = streamId;
    }

    /**
     * Get the ShardId parameter
     * @return ShardId
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * Set the ShardId parameter
     * @param shardId
     */
    public void setShardId(String shardId) {
        Preconditions.checkArgument(shardId != null && !shardId.isEmpty(), "The shardId should not be null or empty.");
        this.shardId = shardId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_SHARD_ITERATOR;
    }

    public boolean hasTimestamp() {
        return timestamp.isValueSet();
    }

    public long getTimestamp() {
        if (!timestamp.isValueSet()) {
            throw new IllegalStateException("The value of Timestamp is not set.");
        }
        return timestamp.getValue();
    }

    public void setTimestamp(long timestamp) {
        this.timestamp.setValue(timestamp);
    }

    public boolean hasToken() {
        return token.isValueSet();
    }

    public String getToken() {
        if (!token.isValueSet()) {
            throw new IllegalStateException("The value of Token is not set.");
        }
        return token.getValue();
    }

    public void setToken(String token) {
        this.token.setValue(token);
    }
}