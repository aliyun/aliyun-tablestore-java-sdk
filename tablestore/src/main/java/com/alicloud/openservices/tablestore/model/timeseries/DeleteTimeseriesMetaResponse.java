package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class DeleteTimeseriesMetaResponse extends Response {

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

    private List<DeleteTimeseriesMetaResponse.FailedRowResult> failedRows;

    public DeleteTimeseriesMetaResponse(Response meta) {
        super(meta);
    }

    public boolean isAllSuccess() {
        return failedRows == null || failedRows.isEmpty();
    }

    public List<DeleteTimeseriesMetaResponse.FailedRowResult> getFailedRows() {
        if (failedRows == null) {
            return new ArrayList<FailedRowResult>();
        }
        return failedRows;
    }

    public void setFailedRows(List<DeleteTimeseriesMetaResponse.FailedRowResult> failedRows) {
        this.failedRows = failedRows;
    }
}
