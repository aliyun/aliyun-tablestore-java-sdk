package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class ScanTimeseriesDataResponse extends Response {

    private List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();
    private byte[] nextToken;

    public ScanTimeseriesDataResponse(Response meta) {
        super(meta);
    }

    public List<TimeseriesRow> getRows() {
        return rows;
    }

    public void setRows(List<TimeseriesRow> rows) {
        this.rows = rows;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }
}
