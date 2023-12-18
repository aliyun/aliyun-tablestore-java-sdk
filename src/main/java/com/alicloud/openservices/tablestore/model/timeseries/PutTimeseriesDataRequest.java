package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PutTimeseriesDataRequest implements Request {

    public enum MetaUpdateMode {
        /**
         * 正常模式，服务端会判断是否更新时间线的元数据索引
         */
        NORMAL,

        /**
         * 不更新时间线元数据的模式
         * 注意，设置为IGNORE后，本次请求写入的数据将不会自动更新时间线的元数据索引，可能会影响元数据检索和SQL查询
         */
        IGNORE
    }

    private final String timeseriesTableName;
    private List<TimeseriesRow> rows = new ArrayList<TimeseriesRow>();

    private MetaUpdateMode metaUpdateMode = MetaUpdateMode.NORMAL;

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

    public MetaUpdateMode getMetaUpdateMode() {
        return metaUpdateMode;
    }

    public void setMetaUpdateMode(MetaUpdateMode metaUpdateMode) {
        this.metaUpdateMode = metaUpdateMode;
    }

    public PutTimeseriesDataRequest createRequestForRetry(List<PutTimeseriesDataResponse.FailedRowResult> failedRows) {
        Preconditions.checkArgument((failedRows != null) && !failedRows.isEmpty(), "failedRows can't be null or empty.");
        PutTimeseriesDataRequest request = new PutTimeseriesDataRequest(timeseriesTableName);
        for (PutTimeseriesDataResponse.FailedRowResult rowResult : failedRows) {
            if (rowResult.getIndex() >= rows.size() || rows.get(rowResult.getIndex()) == null) {
                throw new IllegalArgumentException("Can not find row with index " + rowResult.getIndex());
            }

            request.addRow(rows.get(rowResult.getIndex()));
        }
        request.setMetaUpdateMode(this.getMetaUpdateMode());
        return request;
    }
}
