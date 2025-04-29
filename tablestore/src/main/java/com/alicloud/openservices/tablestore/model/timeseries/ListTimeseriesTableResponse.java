package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ListTimeseriesTableResponse extends Response implements Jsonizable {

    /**
     * Table list.
     */
    private List<TimeseriesTableMeta> timeseriesTableMetas = new ArrayList<TimeseriesTableMeta>();

    public ListTimeseriesTableResponse(Response meta) {
        super(meta);
    }

    /**
     * Get the list of table names.
     * @return The list of table names.
     */
    public List<String> getTimeseriesTableNames() {
        List<String> timeseriesTableNames = new ArrayList<String>();
        for (TimeseriesTableMeta a: timeseriesTableMetas) {
            timeseriesTableNames.add(a.getTimeseriesTableName());
        }
        return timeseriesTableNames;
    }

    public List<TimeseriesTableMeta> getTimeseriesTableMetas() {
        return this.timeseriesTableMetas;
    }

    /*
     * Internal interface. Do not use.
     */
    public void setTimeseriesTableMetas(List<TimeseriesTableMeta> timeseriesTableMetas) {
        this.timeseriesTableMetas = timeseriesTableMetas;
    }


    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        List<String> timeseriesTableNames = getTimeseriesTableNames();
        if (timeseriesTableNames == null) {
            sb.append("null");
        } else {
            sb.append("[");
            Iterator<String> iter = timeseriesTableNames.listIterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
                for (;iter.hasNext();) {
                    sb.append(", \"");
                    sb.append(iter.next());
                    sb.append("\"");
                }
            }
            sb.append("]");
        }
    }
}
