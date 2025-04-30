package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.Collections;
import java.util.List;

public class CreateTimeseriesLastpointIndexRequest implements Request {

    private final String timeseriesTableName;
    private final String lastpointIndexName;
    private final boolean includeBaseData;

    private Boolean createOnWideColumnTable;
    private List<String> lastpointIndexPrimaryKeyNames;

    public CreateTimeseriesLastpointIndexRequest(
            String timeseriesTableName, String lastpointIndexName, boolean includeBaseData) {
        this.timeseriesTableName = timeseriesTableName;
        this.lastpointIndexName = lastpointIndexName;
        this.includeBaseData = includeBaseData;
    }
    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }
    public String getLastpointIndexName() {
        return lastpointIndexName;
    }
    public boolean isIncludeBaseData() {
        return includeBaseData;
    }
    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TIMESERIES_LASTPOINT_INDEX;
    }

    @Override
    public String toString() {
        return "CreateTimeseriesLastpointIndexRequest{"
                + "timeseriesTableName='" + timeseriesTableName + '\''
                + ", lastpointIndexName='" + lastpointIndexName + '\''
                + ", includeBaseData=" + includeBaseData
                + ", createOnWideColumnTable=" + createOnWideColumnTable
                + ", lastpointIndexPrimaryKeyNames=" + lastpointIndexPrimaryKeyNames
                + '}';
    }

    public Boolean getCreateOnWideColumnTable() {
        return createOnWideColumnTable;
    }

    public void setCreateOnWideColumnTable(Boolean createOnWideColumnTable) {
        this.createOnWideColumnTable = createOnWideColumnTable;
    }

    public List<String> getLastpointIndexPrimaryKeyNames() {
        if (lastpointIndexPrimaryKeyNames == null) {
            return Collections.emptyList();
        }
        return lastpointIndexPrimaryKeyNames;
    }

    public void setLastpointIndexPrimaryKeyNames(List<String> lastpointIndexPrimaryKeyNames) {
        this.lastpointIndexPrimaryKeyNames = lastpointIndexPrimaryKeyNames;
    }
}
