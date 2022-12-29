package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.List;

public class ScanTimeseriesDataRequest implements Request {

    private String timeseriesTableName;
    private TimeseriesScanSplitInfo splitInfo;
    private long beginTimeInUs = -1;
    private long endTimeInUs = -1;
    private List<Pair<String, ColumnType>> fieldsToGet;
    private int limit = -1;
    private byte[] nextToken;

    public ScanTimeseriesDataRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SCAN_TIMESERIES_DATA;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public void setTimeseriesTableName(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    public TimeseriesScanSplitInfo getSplitInfo() {
        return splitInfo;
    }

    public void setSplitInfo(TimeseriesScanSplitInfo splitInfo) {
        this.splitInfo = splitInfo;
    }

    public void setTimeRange(long beginTimeInUs, long endTimeInUs) {
        Preconditions.checkArgument(beginTimeInUs >= 0, "begin time must be large than or equal to 0");
        Preconditions.checkArgument(endTimeInUs > beginTimeInUs, "end time must be large than begin time");
        this.beginTimeInUs = beginTimeInUs;
        this.endTimeInUs = endTimeInUs;
    }

    public long getBeginTimeInUs() {
        return beginTimeInUs;
    }

    public void setBeginTimeInUs(long beginTimeInUs) {
        this.beginTimeInUs = beginTimeInUs;
    }

    public long getEndTimeInUs() {
        return endTimeInUs;
    }

    public void setEndTimeInUs(long endTimeInUs) {
        this.endTimeInUs = endTimeInUs;
    }

    public List<Pair<String, ColumnType>> getFieldsToGet() {
        if (fieldsToGet == null) {
            fieldsToGet = new ArrayList<Pair<String, ColumnType>>();
        }
        return fieldsToGet;
    }

    public void setFieldsToGet(List<Pair<String, ColumnType>> fieldsToGet) {
        this.fieldsToGet = fieldsToGet;
    }

    public void addFieldToGet(String fieldName, ColumnType fieldType) {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(fieldType);
        if (fieldsToGet == null) {
            fieldsToGet = new ArrayList<Pair<String, ColumnType>>();
        }
        fieldsToGet.add(new Pair<String, ColumnType>(fieldName, fieldType));
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }
}
