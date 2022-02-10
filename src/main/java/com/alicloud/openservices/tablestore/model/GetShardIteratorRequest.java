package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetShardIteratorRequest implements Request {

    /**
     * Stream的Id
     */
    private String streamId;

    /**
     * Shard的Id
     */
    private String shardId;

    /**
     * 根据时间获取ShardIterator, 时间单位us。
     */
    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * 用于分页获取
     */
    private OptionalValue<String> token = new OptionalValue<String>("Token");

    public GetShardIteratorRequest(String streamId, String shardId) {
        setStreamId(streamId);
        setShardId(shardId);
    }

    /**
     * 获取StreamId参数
     * @return StreamId
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * 设置StreamId参数
     * @param streamId
     */
    public void setStreamId(String streamId) {
        Preconditions.checkArgument(streamId != null && !streamId.isEmpty(), "The streamId should not be null or empty.");
        this.streamId = streamId;
    }

    /**
     * 获取ShardId参数
     * @return ShardId
     */
    public String getShardId() {
        return shardId;
    }

    /**
     * 设置ShardId参数
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