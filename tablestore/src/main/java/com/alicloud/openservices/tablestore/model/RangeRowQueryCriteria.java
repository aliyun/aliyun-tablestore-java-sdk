package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class RangeRowQueryCriteria extends RowQueryCriteria {
    
    private Direction direction = Direction.FORWARD;
    
    /**
     * The maximum number of rows to return.
     */
    private int limit = -1;
    
    private PrimaryKey inclusiveStartPrimaryKey;
    
    private PrimaryKey exclusiveEndPrimaryKey;

    /**
     * For in-row streaming read, marks position and status information.
     */
    private OptionalValue<byte[]> token = new OptionalValue<byte[]>("Token");


    /**
     * Constructs a query condition for a table with the given name.
     * @param tableName
     *          The name of the table to query.
     */
    public RangeRowQueryCriteria(String tableName){
        super(tableName);
    }

    /**
     * Get the maximum number of rows returned during the operation.
     * -1 means returning all rows within the result (default value).
     * @return The maximum number of rows returned in this operation.
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Set the number of rows returned per request during the query.
     * -1 indicates all data rows that meet the conditions (default value).
     * @param limit The number of rows returned per request.
     */
    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "The limit must be greater than 0.");
        this.limit = limit;
    }

    /**
     * Get the read order of the range query (forward(FORWARD) or backward(BACKWARD)).
     * @return read order
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Set the read order (forward(FORWARD) or backward(BACKWARD)) for the range query.
     * @param direction Read order
     */
    public void setDirection(Direction direction) {
        Preconditions.checkNotNull(direction, "The direction should not be null.");
        this.direction = direction;
    }
    
    /**
     * Get the primary key value of the left boundary for the range query.
     * @return The primary key value of the left boundary for the range query.
     */
    public PrimaryKey getInclusiveStartPrimaryKey() {
        return inclusiveStartPrimaryKey;
    }

    /**
     * A range query requires the user to specify a range of primary keys. This range is a half-open interval that is closed on the left and open on the right, with inclusiveStartPrimaryKey as the left boundary of the interval.
     * If direction is FORWARD, inclusiveStartPrimaryKey must be less than exclusiveEndPrimaryKey.
     * If direction is BACKWARD, inclusiveStartPrimaryKey must be greater than exclusiveEndPrimaryKey.
     * inclusiveStartPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the entire range of possible values for that column.
     * @param inclusiveStartPrimaryKey The primary key value for the left boundary of the range query.
     */
    public void setInclusiveStartPrimaryKey(PrimaryKey inclusiveStartPrimaryKey) {
        Preconditions.checkArgument(inclusiveStartPrimaryKey != null && !inclusiveStartPrimaryKey.isEmpty(), "The inclusive start primary key should not be null.");
        this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    }

    /**
     * Get the primary key value of the right boundary for the range query.
     * @return The primary key value of the right boundary for the range query.
     */
    public PrimaryKey getExclusiveEndPrimaryKey() {
        return exclusiveEndPrimaryKey;
    }

    /**
     * Range queries require the user to specify a range for the primary key. This range is a half-open interval that is closed on the left and open on the right, with exclusiveEndPrimaryKey being the right boundary of the interval.
     * If direction is FORWARD, then exclusiveEndPrimaryKey must be greater than inclusiveStartPrimaryKey.
     * If direction is BACKWARD, then exclusiveEndPrimaryKey must be less than inclusiveStartPrimaryKey.
     * exclusiveEndPrimaryKey must include all primary key columns defined in the table. The values of the columns can be defined as {@link PrimaryKeyValue#INF_MIN} or {@link PrimaryKeyValue#INF_MAX} to represent the full range of possible values for that column.
     * @param exclusiveEndPrimaryKey The primary key value for the right boundary of the range query.
     */
    public void setExclusiveEndPrimaryKey(PrimaryKey exclusiveEndPrimaryKey) {
        Preconditions.checkArgument(exclusiveEndPrimaryKey != null && !exclusiveEndPrimaryKey.isEmpty(), "The exclusive end primary key should not be null.");
        this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    }

    public byte[] getToken() {
        if (!this.token.isValueSet()) {
            throw new IllegalStateException("The value of token is not set.");
        }
        return token.getValue();
    }

    public void setToken(byte[] token) {
        if (token != null) {
            this.token.setValue(token);
        }
    }

    public boolean hasSetToken() {
        return this.token.isValueSet();
    }
}
