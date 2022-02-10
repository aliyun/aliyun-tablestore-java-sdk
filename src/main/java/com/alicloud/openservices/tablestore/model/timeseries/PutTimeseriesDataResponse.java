package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class PutTimeseriesDataResponse extends Response {

    public static class FailedRowResult {
        private int index;
        private Error error;

        public FailedRowResult(int index, Error error) {
            this.index = index;
            this.error = error;
        }

        public int getIndex() {
            return index;
        }

        public Error getError() {
            return error;
        }
    }

    private List<FailedRowResult> failedRows;

    public PutTimeseriesDataResponse(Response meta) {
        super(meta);
    }

    public boolean isAllSuccess() {
        return failedRows == null || failedRows.isEmpty();
    }

    public List<FailedRowResult> getFailedRows() {
        if (failedRows == null) {
            return new ArrayList<PutTimeseriesDataResponse.FailedRowResult>();
        }
        return failedRows;
    }

    public void setFailedRows(List<FailedRowResult> failedRows) {
        this.failedRows = failedRows;
    }
}
