package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetStreamRecordRequest implements Request {

    /**
     * Get the records under the Shard through the ShardIterator
     */
    private String shardIterator;

    /**
     * Limit restricts the maximum number of records returned, optional parameter.
     * Default value -1, indicating that the parameter is not set.
     */
    private OptionalValue<Integer> limit = new OptionalValue<Integer>("Limit");

    /**
     * If the table is in a TableGroup, the tableName needs to be provided. For tables not in a TableGroup, this field is optional (it is recommended not to fill it).
     */
    private String tableName;

    /**
     * Whether to parse the data in the format of time-series data. Default is false.
     */
    private boolean parseInTimeseriesDataFormat = false;

    public GetStreamRecordRequest(String shardIterator) {
        setShardIterator(shardIterator);
    }

    /**
     * Set ShardIterator
     * @param shardIterator
     */
    public void setShardIterator(String shardIterator) {
        Preconditions.checkArgument(shardIterator != null && !shardIterator.isEmpty(),
                "The shard iterator is null or empty.");
        this.shardIterator = shardIterator;
    }

    /**
     * Get ShardIterator
     * @return shardIterator
     */
    public String getShardIterator() {
        return shardIterator;
    }

    /**
     * Get the Limit parameter
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
     * Set the Limit parameter
     * @param limit
     */
    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "The limit must be greater than 0.");
        this.limit.setValue(limit);
    }

    /**
     * Get the parseInTimeseriesDataFormat parameter
     * @return parseInTimeseriesDataFormat
     */
    public boolean isParseInTimeseriesDataFormat() {
        return parseInTimeseriesDataFormat;
    }

    /**
     * Set the parseInTimeseriesDataFormat parameter
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
