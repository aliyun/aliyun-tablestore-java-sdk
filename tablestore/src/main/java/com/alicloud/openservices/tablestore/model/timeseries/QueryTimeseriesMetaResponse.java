package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class QueryTimeseriesMetaResponse extends Response {

    private List<TimeseriesMeta> timeseriesMetas = new ArrayList<TimeseriesMeta>();
    private long totalHits = -1;
    private byte[] nextToken;

    public QueryTimeseriesMetaResponse(Response meta) {
        super(meta);
    }

    public List<TimeseriesMeta> getTimeseriesMetas() {
        return timeseriesMetas;
    }

    public void setTimeseriesMetas(List<TimeseriesMeta> timeseriesMetas) {
        this.timeseriesMetas = timeseriesMetas;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public void setTotalHits(long totalHits) {
        this.totalHits = totalHits;
    }
}
