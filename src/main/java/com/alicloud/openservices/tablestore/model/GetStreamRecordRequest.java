package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetStreamRecordRequest implements Request {

    /**
     * 通过ShardIterator获取该Shard下的记录
     */
    private String shardIterator;

    /**
     * limit限制返回的记录的最大条数，可选参数
     * 默认值－1，代表该参数未设置。
     */
    private OptionalValue<Integer> limit = new OptionalValue<Integer>("Limit");

    public GetStreamRecordRequest(String shardIterator) {
        setShardIterator(shardIterator);
    }

    /**
     * 设置ShardIterator
     * @param shardIterator
     */
    public void setShardIterator(String shardIterator) {
        Preconditions.checkArgument(shardIterator != null && !shardIterator.isEmpty(),
                "The shard iterator is null or empty.");
        this.shardIterator = shardIterator;
    }

    /**
     * 获取ShardIterator
     * @return shardIterator
     */
    public String getShardIterator() {
        return shardIterator;
    }

    /**
     * 获取Limit参数
     * @return limit
     */
    public int getLimit() {
        if (limit.isValueSet()) {
            return limit.getValue();
        } else {
            return -1;
        }
    }

    /**
     * 设置Limit参数
     * @param limit
     */
    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "The limit must be greater than 0.");
        this.limit.setValue(limit);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_STREAM_RECORD;
    }
}
