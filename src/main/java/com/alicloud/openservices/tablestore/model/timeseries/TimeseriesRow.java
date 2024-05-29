package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TimeseriesRow {

    private TimeseriesKey timeseriesKey;
    private long timeInUs = -1;
    private final SortedMap<String, ColumnValue> fields = new TreeMap<String, ColumnValue>();

    public TimeseriesRow(TimeseriesKey timeseriesKey) {
        this.timeseriesKey = timeseriesKey;
    }

    public TimeseriesRow(TimeseriesKey timeseriesKey, long timeInUs) {
        this.timeseriesKey = timeseriesKey;
        this.timeInUs = timeInUs;
    }

    public int getTimeseriesRowDataSize() {
        int totalSize = 0;
        totalSize += 8;     // time size
        totalSize += CalculateHelper.calcStringSizeInBytes(timeseriesKey.getMeasurementName());
        totalSize += CalculateHelper.calcStringSizeInBytes(timeseriesKey.getDataSource());
        for (Map.Entry<String, String> entry : timeseriesKey.getTags().entrySet()) {
            totalSize += CalculateHelper.calcStringSizeInBytes(entry.getKey());
            totalSize += CalculateHelper.calcStringSizeInBytes(entry.getValue());
        }
        for (Map.Entry<String, ColumnValue> entry : fields.entrySet()) {
            totalSize += entry.getValue().getDataSize() + CalculateHelper.calcStringSizeInBytes(entry.getKey());
        }
        return totalSize;
    }

    public TimeseriesKey getTimeseriesKey() {
        return timeseriesKey;
    }

    public void setTimeseriesKey(TimeseriesKey timeseriesKey) {
        this.timeseriesKey = timeseriesKey;
    }

    public long getTimeInUs() {
        return timeInUs;
    }

    public void setTimeInUs(long timeInUs) {
        Preconditions.checkArgument(timeInUs >= 0, "time can not be negative");
        this.timeInUs = timeInUs;
    }

    public SortedMap<String, ColumnValue> getFields() {
        return fields;
    }

    public void setFields(Map<String, ColumnValue> fields) {
        this.fields.clear();
        this.fields.putAll(fields);
    }

    public void addField(String key, ColumnValue value) {
        this.fields.put(key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeseriesRow that = (TimeseriesRow) o;
        return Objects.equal(timeseriesKey, that.timeseriesKey) && Objects.equal(timeInUs, that.timeInUs) && Objects.equal(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timeseriesKey, timeInUs, fields);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
