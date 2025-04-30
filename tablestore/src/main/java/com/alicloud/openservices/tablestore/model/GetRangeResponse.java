package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.List;

public class GetRangeResponse extends Response {
    /**
     * The capacity unit consumed by this operation.
     */
    private ConsumedCapacity consumedCapacity;

    /**
     * All rows returned by the range query.
     */
    private List<Row> rows;

    /**
     * The start boundary of the next query range.
     */
    private PrimaryKey nextStartPrimaryKey;

    private byte[] nextToken;

    private long bodyBytes;

    /**
     * internal use
     */
    public GetRangeResponse(Response meta, ConsumedCapacity consumedCapacity) {
        super(meta);
        Preconditions.checkNotNull(consumedCapacity);
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * internal use
     */
    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    /**
     * internal use
     */
    public void setNextStartPrimaryKey(PrimaryKey nextStartPrimaryKey) {
        this.nextStartPrimaryKey = nextStartPrimaryKey;
    }

    /**
     * Get the CapacityUnit consumed by this operation.
     *
     * @return The CapacityUnit consumed by this operation.
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    /**
     * Get all rows returned by this query.
     *
     * @return all rows
     */
    public List<Row> getRows() {
        return rows;
    }

    /**
     * Get the start boundary of the next query range.
     * If it is null, it means that this query has already returned all the data, and there is no need for another query.
     *
     * @return If the data within this range has not been fully read, return the primary key of the next row; otherwise, return null.
     */
    public PrimaryKey getNextStartPrimaryKey() {
        return nextStartPrimaryKey;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }

    public boolean hasNextToken() {
        return (nextToken != null) && (nextToken.length > 0);
    }

    public long getBodyBytes() {
        return bodyBytes;
    }

    public void setBodyBytes(long bodyBytes) {
        this.bodyBytes = bodyBytes;
    }
}
