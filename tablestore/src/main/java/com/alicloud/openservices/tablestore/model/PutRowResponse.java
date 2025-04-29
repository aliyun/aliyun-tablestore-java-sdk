package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;


public class PutRowResponse extends Response implements Jsonizable {
    /**
     * The capacity unit consumed by this operation.
     */
    private ConsumedCapacity consumedCapacity;
    
    /**
     * ReturnType specifies the returned value.
     */
    private Row row;

    public PutRowResponse(Response meta, Row row, ConsumedCapacity consumedCapacity) {
        super(meta);
        Preconditions.checkNotNull(consumedCapacity);
        this.consumedCapacity = consumedCapacity;
        this.row = row;
    }

    /**
     * Get the capacity unit consumed by this operation.
     *
     * @return The capacity unit consumed by this operation.
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
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

    public Row getRow() {
        return row;
    }
}
