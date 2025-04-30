package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

public class DeleteTimeseriesLastpointIndexResponse extends Response {
    public DeleteTimeseriesLastpointIndexResponse(Response meta) {
        super(meta);
    }

    @Override
    public String toString() {
        return "DeleteTimeseriesLastpointIndexResponse [requestId=" + getRequestId() + "]";
    }
}
