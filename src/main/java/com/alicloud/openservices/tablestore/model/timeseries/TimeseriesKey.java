package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TimeseriesKey implements Comparable<TimeseriesKey> {

    private final String measurementName;
    private final String dataSource;
    private final SortedMap<String, String> tags = new TreeMap<String, String>();
    private String tagsString;

    public TimeseriesKey(String measurementName, String dataSource) {
        this(measurementName, dataSource, null);
    }

    public TimeseriesKey(String measurementName, Map<String, String> tags) {
        this(measurementName, "", tags);
    }

    public TimeseriesKey(String measurementName, String dataSource, Map<String, String> tags) {
        Preconditions.checkStringNotNullAndEmpty(measurementName, "measurement should not be null or empty");
        this.measurementName = measurementName;
        if (dataSource != null) {
            this.dataSource = dataSource;
        } else {
            this.dataSource = "";
        }
        if (tags != null) {
            this.tags.putAll(tags);
        }
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public String getDataSource() {
        return dataSource;
    }

    public SortedMap<String, String> getTags() {
        return Collections.unmodifiableSortedMap(tags);
    }

    public String buildTagsString() {
        if (tagsString == null) {
            tagsString = TimeseriesProtocolBuilder.buildTagsString(this.tags);
            return tagsString;
        }
        return tagsString;
    }

    public String buildMetaCacheKey(String tableName) {
        StringBuilder sb = new StringBuilder(tableName.length() + measurementName.length() +
                dataSource.length() + buildTagsString().length() + 3);
        sb.append(tableName);
        sb.append('\t');
        sb.append(measurementName);
        sb.append('\t');
        sb.append(dataSource);
        sb.append('\t');
        sb.append(buildTagsString());
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeseriesKey that = (TimeseriesKey) o;
        return Objects.equal(measurementName, that.measurementName) && Objects.equal(dataSource, that.dataSource) && Objects.equal(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(measurementName, dataSource, tags);
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }


    @Override
    public int compareTo(TimeseriesKey target) {

        int ret = this.measurementName.compareTo(target.measurementName);
        if (ret != 0) {
            return ret;
        }
        ret = this.dataSource.compareTo(target.dataSource);
        if (ret != 0) {
            return ret;
        }
        ret = this.tagsString.compareTo(target.tagsString);
        return ret;
    }
}
