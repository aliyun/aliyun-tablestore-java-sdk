package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;

import java.util.List;

/**
 * SearchIndex的返回结果
 */
public class SearchResponse extends Response {

    /**
     *  根据输入的Query语句进行查询，SearchIndex引擎返回的总命中数
     *  <p>注：是查询到的实际数量，不是该Response中返回的具体的行数。行数可以由其他参数来控制，进行类似分页的操作</p>
     */
    private long totalCount;

    /**
     * Query查询的具体返回结果列表
     */
    private List<Row> rows;

    /**
     * 是否查询成功
     */
    private boolean isAllSuccess;

    private byte[] nextToken;

    public SearchResponse(Response meta) {
        super(meta);
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public List<Row> getRows() {
        return rows;
    }

    public void setRows(List<Row> rows) {
        this.rows = rows;
    }

    public boolean isAllSuccess() {
        return isAllSuccess;
    }

    public void setAllSuccess(boolean allSuccess) {
        isAllSuccess = allSuccess;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }
}
