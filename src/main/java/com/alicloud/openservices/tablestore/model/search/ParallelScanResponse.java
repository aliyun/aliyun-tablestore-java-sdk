package com.alicloud.openservices.tablestore.model.search;

import java.util.List;

import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByResults;
import com.google.gson.GsonBuilder;

public class ParallelScanResponse extends Response {

    private List<Row> rows;

    private byte[] nextToken;

    private long bodyBytes;

    public ParallelScanResponse(Response meta) {
        super(meta);
    }

    public ParallelScanResponse() {
    }

    public List<Row> getRows() {
        return rows;
    }

    public ParallelScanResponse setRows(List<Row> rows) {
        this.rows = rows;
        return this;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public ParallelScanResponse setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
        return this;
    }

    public String getResponseInfo(boolean prettyFormat) {
        GsonBuilder builder = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization();
        if (prettyFormat) {
            return builder.setPrettyPrinting().create().toJson(this);
        } else {
            return builder.create().toJson(this);
        }
    }

    public void printResponseInfo() {
        System.out.println(getResponseInfo(true));
    }

    public long getBodyBytes() {
        return bodyBytes;
    }

    public void setBodyBytes(long bodyBytes) {
        this.bodyBytes = bodyBytes;
    }
}
