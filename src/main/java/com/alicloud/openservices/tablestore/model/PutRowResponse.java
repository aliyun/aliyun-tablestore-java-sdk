package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;


public class PutRowResponse extends Response implements Jsonizable {
    /**
     * 此次操作消耗的能力单元。
     */
    private ConsumedCapacity consumedCapacity;
    
    /**
     * ReturnType指定返回的值。
     */
    private Row row;

    public PutRowResponse(Response meta, Row row, ConsumedCapacity consumedCapacity) {
        super(meta);
        Preconditions.checkNotNull(consumedCapacity);
        this.consumedCapacity = consumedCapacity;
        this.row = row;
    }

    /**
     * 获取此次操作消耗的能力单元。
     *
     * @return 此次操作消耗的能力单元。
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
