package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.List;

public class GetRangeResponse extends Response {
    /**
     * 此次操作消耗的能力单元。
     */
    private ConsumedCapacity consumedCapacity;

    /**
     * 范围查询返回的所有行。
     */
    private List<Row> rows;

    /**
     * 下一次查询的范围的起始边界。
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
     * 获取此次操作消耗的CapacityUnit。
     *
     * @return 此次操作消耗的CapacityUnit。
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    /**
     * 获取本次查询返回的所有行。
     *
     * @return 所有行
     */
    public List<Row> getRows() {
        return rows;
    }

    /**
     * 获取下一次查询的范围的起始边界。
     * 若为null，则代表本次查询已经返回所有数据，无需再次查询。
     *
     * @return 若该范围内的数据还未读取完毕，则返回下一行的主键，否则返回null
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
