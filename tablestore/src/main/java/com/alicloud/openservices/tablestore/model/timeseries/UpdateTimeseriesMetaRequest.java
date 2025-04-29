package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.List;

public class UpdateTimeseriesMetaRequest implements Request {

    private final String timeseriesTableName;
    private List<TimeseriesMeta> metas = new ArrayList<TimeseriesMeta>();

    public UpdateTimeseriesMetaRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_TIMESERIES_META;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public List<TimeseriesMeta> getMetas() {
        return metas;
    }

    public void setMetas(List<TimeseriesMeta> metas) {
        this.metas = metas;
    }
}
