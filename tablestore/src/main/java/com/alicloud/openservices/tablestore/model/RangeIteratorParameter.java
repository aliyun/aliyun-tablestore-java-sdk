package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RangeIteratorParameter extends RowQueryCriteria {

    private Direction direction = Direction.FORWARD;

    /**
     * The maximum number of rows returned by this Iterator query. If count is not set, it returns all rows within the query range.
     * The default value -1 means no limit on the number of rows, and reads all rows in this range.
     */
    private int maxCount = -1;

    /**
     * The size of the buffer used for iterator-based batch queries. This size determines the maximum number of rows returned in each request when the Iterator calls GetRange.
     * A default value of -1 means the buffer size is not set, and each request will return the maximum number of rows allowed by TableStore.
     */
    private int bufferSize = -1;

    private PrimaryKey inclusiveStartPrimaryKey;

    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * Constructs a query condition for a table with the given name.
     *
     * @param tableName The name of the table to query.
     */
    public RangeIteratorParameter(String tableName) {
        super(tableName);
    }

    public RangeIteratorParameter(RangeRowQueryCriteria criteria) {
        super(criteria.getTableName());

        criteria.copyTo(this);
        this.direction = criteria.getDirection();
        this.bufferSize = criteria.getLimit();
        this.inclusiveStartPrimaryKey = criteria.getInclusiveStartPrimaryKey();
        this.exclusiveEndPrimaryKey = criteria.getExclusiveEndPrimaryKey();
    }

    /**
     * The maximum number of rows that the iterator can return. 
     * -1 means returning all rows within this range (default value).
     *
     * @return The maximum number of rows returned by this operation.
     */
    public int getMaxCount() {
        return maxCount;
    }

    /**
     * Set the maximum number of rows that this Iterator will return.
     * -1 means returning all rows within this range (default value).
     *
     * @param maxCount The number of rows to be returned in a single request.
     */
    public void setMaxCount(int maxCount) {
        Preconditions.checkArgument(maxCount > 0, "The max count must be greater than 0.");
        this.maxCount = maxCount;
    }

    /**
     * Get the size of the internal Buffer.
     *
     * @return The size of the Buffer.
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Set the size of the Buffer.
     *
     * @param bufferSize The size of the Buffer.
     */
    public void setBufferSize(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0, "The buffer size must be greater than 0.");
        this.bufferSize = bufferSize;
    }

    /**
     * Get the read order of the range query (forward(FORWARD) or backward(BACKWARD)).
     *
     * @return Read order
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Set the read order (FORWARD or BACKWARD) for the range query.
     *
     * @param direction The read order
     */
    public void setDirection(Direction direction) {
        Preconditions.checkNotNull(direction, "The direction should not be null.");
        this.direction = direction;
    }

    /**
     * Gets the primary key value of the left boundary for the range query.
     *
     * @return The primary key value of the left boundary for the range query.
     */
    public PrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * A range query requires the user to specify a range of primary keys. This range is a half-open interval that is closed on the left and open on the right. The inclusiveStartPrimaryKey represents the left boundary of this interval.
     * If direction is FORWARD, then inclusiveStartPrimaryKey must be less than exclusiveEndPrimaryKey.
     * If direction is BACKWARD, then inclusiveStartPrimaryKey must be greater than exclusiveEndPrimaryKey.
     * inclusiveStartPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the entire range of possible values for that column.
     *
     * @param inclusiveStartPrimaryKey The primary key value for the left boundary of the range query.
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The start primary key should not be null or empty.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * Get the primary key value of the right boundary for the range query.
     *
     * @return The primary key value of the right boundary for the range query.
     */
    public PrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * Range queries require users to specify a range of primary keys. This range is a half-open interval that is closed on the left and open on the right, with exclusiveEndPrimaryKey being the right boundary of the interval.
     * If direction is FORWARD, exclusiveEndPrimaryKey must be greater than inclusiveStartPrimaryKey.
     * If direction is BACKWARD, exclusiveEndPrimaryKey must be less than inclusiveStartPrimaryKey.
     * exclusiveEndPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the entire range of possible values for that column.
     *
     * @param exclusiveEndPrimaryKey The primary key value representing the right boundary of the range query.
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The end primary key should not be null or empty.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }
}
