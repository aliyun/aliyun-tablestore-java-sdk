package com.alicloud.openservices.tablestore.model;

import java.util.List;

public class DescribeStreamResponse extends Response {

    /**
     * Stream的Id
     */
    private String streamId;

    /**
     * Stream的过期时间
     */
    private int expirationTime;

    /**
     * 所属的表的名称
     */
    private String tableName;

    /**
     * Stream的创建时间
     */
    private long creationTime;

    /**
     * Stream的状态
     */
    private StreamStatus status;

    /**
     * Shard列表
     */
    private List<StreamShard> shards;

    /**
     * 用于下一次请求时指定返回Shard列表的左边界(InclusiveStartShardId)
     */
    private String nextShardId;

    private boolean isTimeseriesDataTable;

    public DescribeStreamResponse() {

    }

    public DescribeStreamResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取Stream的Id
     * @return Stream的Id
     */
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * 获取表的名称
     * @return 表的名称
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 获取Shard列表
     * @return Shard列表
     */
    public List<StreamShard> getShards() {
        return shards;
    }

    public void setShards(List<StreamShard> shards) {
        this.shards = shards;
    }

    /**
     * 获取NextShardId参数
     * @return NextShardId参数
     */
    public String getNextShardId() {
        return nextShardId;
    }

    public void setNextShardId(String nextShardId) {
        this.nextShardId = nextShardId;
    }

    /**
     * 获取Stream的状态
     * @return Stream的状态
     */
    public StreamStatus getStatus() {
        return status;
    }

    public void setStatus(StreamStatus status) {
        this.status = status;
    }

    /**
     * 获取Stream的过期时间，单位小时
     * @return Stream的过期时间
     */
    public int getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * 获取Stream的创建时间，单位微秒
     * @return Stream的创建时间，单位微秒
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

