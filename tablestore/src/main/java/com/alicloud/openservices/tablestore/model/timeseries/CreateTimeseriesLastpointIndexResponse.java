package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.Response;

public class CreateTimeseriesLastpointIndexResponse extends Response {
    public CreateTimeseriesLastpointIndexResponse(Response meta) {
        super(meta);
    }

    @Override
    public String toString() {
        return "CreateTimeseriesLastpointIndexResponse [requestId=" + getRequestId() + "]";
    }
}
