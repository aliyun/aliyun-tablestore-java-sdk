package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.nio.ByteBuffer;

public class BulkExportResponse extends Response {
    private ConsumedCapacity consumedCapacity;

    private ByteBuffer rows;

    private PrimaryKey nextStartPrimaryKey;

    private byte[] nextToken;

    private DataBlockType dataBlockType = DataBlockType.DBT_PLAIN_BUFFER;

    private long bodyBytes;

    public BulkExportResponse(Response meta, ConsumedCapacity consumedCapacity) {
        super(meta);
        Preconditions.checkNotNull(consumedCapacity);
        this.consumedCapacity = consumedCapacity;
    }

    public void setRows(ByteBuffer rows) {
        this.rows = rows;
    }

    public void setNextStartPrimaryKey(PrimaryKey nextStartPrimaryKey) {
        this.nextStartPrimaryKey = nextStartPrimaryKey;
    }

    public void setDataBlockType(DataBlockType dataBlockType) {
        this.dataBlockType = dataBlockType;
    }

    public DataBlockType getDataBlockType() {
        return dataBlockType;
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
    public ByteBuffer getRows() {
        return rows;
    }

    /**
     * Get the start boundary of the next query range.
     * If it is null, it means that this query has already returned all the data, and there is no need to query again.
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