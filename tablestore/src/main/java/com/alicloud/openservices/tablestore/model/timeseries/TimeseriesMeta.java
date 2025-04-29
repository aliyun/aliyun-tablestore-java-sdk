package com.alicloud.openservices.tablestore.model.timeseries;

import com.google.gson.Gson;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TimeseriesMeta {

    private TimeseriesKey timeseriesKey;
    private SortedMap<String, String> attributes = new TreeMap<String, String>();
    private long updateTimeInUs = -1;

    public TimeseriesMeta(TimeseriesKey timeseriesKey) {
        this.timeseriesKey = timeseriesKey;
    }

    public TimeseriesKey getTimeseriesKey() {
        return timeseriesKey;
    }

    public void setTimeseriesKey(TimeseriesKey timeseriesKey) {
        this.timeseriesKey = timeseriesKey;
    }

    public SortedMap<String, String> getAttributes() {
        return Collections.unmodifiableSortedMap(attributes);
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes.clear();
        this.attributes.putAll(attributes);
    }

    public long getUpdateTimeInUs() {
        return updateTimeInUs;
    }

    public void setUpdateTimeInUs(long updateTimeInUs) {
        this.updateTimeInUs = updateTimeInUs;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
