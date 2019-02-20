package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class GetRowResponse extends Response implements Jsonizable {
    /**
     * 单行查询返回的数据。
     */
    private Row row;

    /**
     * 此次操作消耗的能力单元。
     */
    private ConsumedCapacity consumedCapacity;

    private byte[] nextToken;

    public GetRowResponse(Response meta, Row row, ConsumedCapacity consumedCapacity) {
        super(meta);
        this.row = row;
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * 获取单行查询返回的数据。
     *
     * @return 若查询的该行存在，则返回该行，否则返回null
     */
    public Row getRow() {
        return row;
    }

    /**
     * 获取此次操作消耗的能力单元。
     *
     * @return 此次操作消耗的能力单元。
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
