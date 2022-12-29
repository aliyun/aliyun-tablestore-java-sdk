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

    /**
     * 若表为TableGroup中的表，则需要传入tableName。非TableGroup表，此字段填不填都可以（建议不填）。
     */
    private String tableName;

    /**
     * 是否将数据按照时序数据的格式解析，默认为false。
     */
    private boolean parseInTimeseriesDataFormat = false;

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

    /**
     * 获取parseInTimeseriesDataFormat参数
     * @return parseInTimeseriesDataFormat
     */
    public boolean isParseInTimeseriesDataFormat() {
        return parseInTimeseriesDataFormat;
    }

    /**
     * 设置parseInTimeseriesDataFormat参数
     * @param parseInTimeseriesDataFormat
     */
    public void setParseInTimeseriesDataFormat(boolean parseInTimeseriesDataFormat) {
        this.parseInTimeseriesDataFormat = parseInTimeseriesDataFormat;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_STREAM_RECORD;
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
                "The table name is null or empty.");
        this.tableName = tableName;
    }
}
