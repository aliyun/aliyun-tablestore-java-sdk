package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class GetRowResponse extends Response implements Jsonizable {
    /**
     * The data returned by a single row query.
     */
    private Row row;

    /**
     * The capacity unit consumed by this operation.
     */
    private ConsumedCapacity consumedCapacity;

    private byte[] nextToken;

    public GetRowResponse(Response meta, Row row, ConsumedCapacity consumedCapacity) {
        super(meta);
        this.row = row;
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * Get the data returned by a single row query.
     *
     * @return If the queried row exists, return the row; otherwise, return null.
     */
    public Row getRow() {
        return row;
    }

    /**
     * Get the capacity unit consumed by this operation.
     *
     * @return The capacity unit consumed by this operation.
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
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

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"ConsumedCapacity\": ");
        consumedCapacity.jsonize(sb, newline + "  ");
        sb.append("}");
    }
}
