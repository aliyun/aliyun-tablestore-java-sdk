package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.google.common.base.Objects;
import com.google.gson.Gson;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class TimeseriesKey implements Comparable<TimeseriesKey> {

    private final String measurementName;
    private final String dataSource;
    private final SortedMap<String, String> tags = new TreeMap<String, String>();

    public TimeseriesKey(String measurementName, String dataSource) {
        this(measurementName, dataSource, null);
    }

    public TimeseriesKey(String measurementName, Map<String, String> tags) {
        this(measurementName, "", tags);
    }

    public TimeseriesKey(String measurementName, String dataSource, Map<String, String> tags) {
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

    public String buildMetaCacheKey(String tableName) {
        int capacity = 0;
        capacity += tableName.length();
        capacity += measurementName.length();
        capacity += dataSource.length();
        capacity += 4;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            capacity += entry.getKey().length() + entry.getValue().length() + 3;
        }
        StringBuilder sb = new StringBuilder(capacity);
        sb.append(tableName);
        sb.append((char)(measurementName.length()));
        sb.append(measurementName);
        sb.append((char)(dataSource.length()));
        sb.append(dataSource);
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            sb.append(entry.getKey());
            sb.append("\t");
            sb.append((char)(entry.getValue().length()));
            sb.append(entry.getValue());
        }
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
        // union of keys
        SortedSet<String> keys = new TreeSet<String>();
        keys.addAll(this.tags.keySet());
        keys.addAll(target.tags.keySet());
        for (String key : keys) {
            String value = this.tags.get(key);
            String targetValue = target.tags.get(key);
            if (value == null && targetValue == null) {
                continue;
            } else if (value == null) {
                return -1;
            } else if (targetValue == null) {
                return 1;
            }
            ret = value.compareTo(targetValue);
            if (ret != 0) {
                return ret;
            }
        }
        return ret;
    }
}
