package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.List;

public class DeleteTimeseriesMetaRequest implements Request {

    private final String timeseriesTableName;
    private List<TimeseriesKey> timeseriesKeys = new ArrayList<TimeseriesKey>();

    public DeleteTimeseriesMetaRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TIMESERIES_META;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public List<TimeseriesKey> getTimeseriesKeys() {
        return timeseriesKeys;
    }

    public void setTimeseriesKeys(List<TimeseriesKey> timeseriesKeys) {
        this.timeseriesKeys = timeseriesKeys;
    }
}
