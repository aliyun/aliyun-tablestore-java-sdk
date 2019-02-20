package com.alicloud.openservices.tablestore.model;


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
}
