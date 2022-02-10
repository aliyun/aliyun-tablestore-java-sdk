package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.ArrayList;
import java.util.List;

public class GetTimeseriesDataRequest implements Request {

    private final String timeseriesTableName;
    private TimeseriesKey timeseriesKey;
    private long beginTimeInUs;
    private long endTimeInUs;
    private int limit = -1;
    private byte[] nextToken;
    private boolean backward;
    private List<Pair<String, ColumnType>> fieldsToGet;

    public GetTimeseriesDataRequest(String timeseriesTableName) {
        this.timeseriesTableName = timeseriesTableName;
    }

    public String getTimeseriesTableName() {
        return timeseriesTableName;
    }

    public TimeseriesKey getTimeseriesKey() {
        return timeseriesKey;
    }

    public void setTimeseriesKey(TimeseriesKey timeseriesKey) {
        this.timeseriesKey = timeseriesKey;
    }

    public void setTimeRange(long beginTimeInUs, long endTimeInUs) {
        Preconditions.checkArgument(beginTimeInUs >= 0, "begin time must be large than or equal to 0");
        Preconditions.checkArgument(endTimeInUs > 0, "end time must be large than 0");
        this.beginTimeInUs = beginTimeInUs;
        this.endTimeInUs = endTimeInUs;
    }

    public long getBeginTimeInUs() {
        return beginTimeInUs;
    }

    public long getEndTimeInUs() {
        return endTimeInUs;
    }

    public void setNextToken(byte[] nextToken) {
        this.nextToken = nextToken;
    }

    public byte[] getNextToken() {
        return nextToken;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_TIMESERIES_DATA;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        Preconditions.checkArgument(limit > 0, "limit must large than 0");
        this.limit = limit;
    }

    public boolean isBackward() {
        return backward;
    }

    /**
     * 是否按照时间逆序读，可用于获取最新的数据
     * @param backward
     */
    public void setBackward(boolean backward) {
        this.backward = backward;
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
}
