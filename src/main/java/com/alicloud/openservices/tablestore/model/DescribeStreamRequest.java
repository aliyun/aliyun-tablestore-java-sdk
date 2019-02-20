package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DescribeStreamRequest implements Request {

    /**
     * StreamId，指定一个Stream。
     */
    private String streamId;

    /**
     * InclusiveStartShardId指定返回的Shard列表的左边界。可选参数。
     */
    private OptionalValue<String> inclusiveStartShardId = new OptionalValue<String>("InclusiveStartShardId");

    /**
     * ShardLimit指定返回的Shard的最大数目。可选参数。
     */
    private OptionalValue<Integer> shardLimit = new OptionalValue<Integer>("ShardLimit");

    public DescribeStreamRequest(String streamId) {
        setStreamId(streamId);
    }

    /**
     * 获取StreamId参数。
     * StreamId用于指定一个Stream，当在一个表上开启Stream时，对应的StreamId会由服务端生成。
     * 用户可以通过ListStream请求获取StreamId。
     *
     * @return streamId
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * 设置StreamId参数。
     * StreamId用于指定一个Stream，当在一个表上开启Stream时，对应的StreamId会由服务端生成。
     * 用户可以通过ListStream请求获取StreamId。
     *
     * @param streamId
     */
    public void setStreamId(String streamId) {
        Preconditions.checkArgument(streamId != null && !streamId.isEmpty(), "The streamId should not be null or empty.");
        this.streamId = streamId;
    }

    /**
     * 获取InclusiveStartShardId参数。
     * InclusiveStartShardId参数用于指定返回的Shard列表的左边界(包含)。
     * 若返回null, 则代表该参数为设置。
     *
     * @return inclusiveStartShardId
     */
    public String getInclusiveStartShardId() {
        return inclusiveStartShardId.getValue();
    }

    /**
     * 设置InclusiveStartShardId参数。
     * InclusiveStartShardId参数用于指定返回的Shard列表的左边界(包含)。
     *
     * @param inclusiveStartShardId
     */
    public void setInclusiveStartShardId(String inclusiveStartShardId) {
        Preconditions.checkArgument(inclusiveStartShardId != null && !inclusiveStartShardId.isEmpty(),
                "The inclusiveStartShardId is null or empty.");
        this.inclusiveStartShardId.setValue(inclusiveStartShardId);
    }

    /**
     * 获取ShardLimit参数。
     * ShardLimit参数用于限制返回的Shard的最大数目。
     *
     * @return ShardLimit参数，若为-1，则代表未设置该参数。
     */
    public int getShardLimit() {
        if (shardLimit.isValueSet()) {
            return shardLimit.getValue();
        } else {
            return -1;
        }
    }

    /**
     * 设置ShardLimit参数。
     * ShardLimit参数用于限制返回的Shard的最大数目。
     *
     * @param shardLimit
     */
    public void setShardLimit(int shardLimit) {
        Preconditions.checkArgument(shardLimit > 0, "The limit must be greater than 0.");
        this.shardLimit.setValue(shardLimit);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_STREAM;
    }
}
