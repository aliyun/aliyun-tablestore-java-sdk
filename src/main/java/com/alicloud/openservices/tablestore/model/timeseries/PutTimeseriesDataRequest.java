package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PutTimeseriesDataRequest implements Request {

    private final String timeseriesTableName;
    private List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();

    public PutTimeseriesDataRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_PUT_TIMESERIES_DATA;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public List<TimeseriesRow> getRows() {
        return rows;
    }

    public void setRows(List<TimeseriesRow> rows) {
        for (TimeseriesRow row : rows) {
            if (row.getTimeInUs() < 0) {
                throw new ClientException("time not set in timeseriesRow");
            }
        }
        this.rows = rows;
    }

    public void addRows(Collection<TimeseriesRow> rows) {
        this.rows.addAll(rows);
    }

    public void addRow(TimeseriesRow row) {
        this.rows.add(row);
    }
}
