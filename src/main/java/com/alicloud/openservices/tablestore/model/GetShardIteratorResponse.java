package com.alicloud.openservices.tablestore.model;


public class GetShardIteratorResponse extends Response {

    /**
     * ShardIterator，用于获取该Shard下的记录
     */
    private String shardIterator;

    private String nextToken;

    public GetShardIteratorResponse() {

    }

    public GetShardIteratorResponse(Response meta) {
        super(meta);
    }

    /**
     * 获取ShardIterator
     * @return ShardIterator
     */
    public String getShardIterator() {
        return shardIterator;
    }

    public void setShardIterator(String shardIterator) {
        this.shardIterator = shardIterator;
    }

    public String getNextToken() {
        return nextToken;
    }

    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }
}