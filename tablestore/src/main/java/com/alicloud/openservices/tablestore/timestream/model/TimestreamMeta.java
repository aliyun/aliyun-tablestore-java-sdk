package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.timestream.TimestreamRestrict;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.internal.Utils;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Timeline, includes:
 * 1. {@link TimestreamIdentifier}: Unique identifier for the timeline.
 * 2. updateTime (update time): The update time of the timeline, which is automatically updated by the backend when data is written.
 * 3. attributes: Mutable attributes of the timeline; changes to these attributes do not affect the confirmation of the timeline.
 */
public class TimestreamMeta {
    private TimestreamIdentifier identifier;
    private long updateTime;
    private Map<String, ColumnValue> attributes = new TreeMap<String, ColumnValue>();

    public TimestreamMeta(TimestreamIdentifier identifier) {
        this.identifier = identifier;
        this.updateTime = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    protected static TimestreamMeta newInstance(Row row) {
        return  Utils.deserializeTimestreamMeta(row);
    }

    public TimestreamIdentifier getIdentifier() {
        return this.identifier;
    }

    public TimestreamMeta setUpdateTime(long timestamp, TimeUnit unit) {
        this.updateTime = unit.toMicros(timestamp);
        return this;
    }

    public long getUpdateTimeInUsec() {
        return this.updateTime;
    }

    /**
     * The key of attribute cannot be a reserved field
     * @param key
     */
    private void checkKeyParams(String key) {
        if (key.equals(TableMetaGenerator.CN_TAMESTAMP_NAME)) {
            throw new ClientException("Key of attribute cannot be " + TableMetaGenerator.CN_TAMESTAMP_NAME + ".");
        }
    }

    /**
     * Set attributes
     * @param attributes
     * @return
     */
    public TimestreamMeta setAttributes(TreeMap<String, ColumnValue> attributes) {
        for (String key : attributes.keySet()) {
            checkKeyParams(key);
        }
        this.attributes = attributes;
        return this;
    }

    public TimestreamMeta addAttribute(String name, ColumnValue value) {
        checkKeyParams(name);
        this.attributes.put(name, value);
        return this;
    }

    public TimestreamMeta addAttribute(String name, Location value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromString(value.toString()));
        return this;
    }

    /**
     * Add an attribute of String type
     * @param name
     * @param value
     * @return
     */
    public TimestreamMeta addAttribute(String name, String value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromString(value));
        return this;
    }

    /**
     * Add an attribute of Long type
     * @param name
     * @param value
     * @return
     */
    public TimestreamMeta addAttribute(String name, long value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromLong(value));
        return this;
    }

    /**
     * Add an attribute of Binary type
     * @param name
     * @param value
     * @return
     */
    public TimestreamMeta addAttribute(String name, byte[] value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromBinary(value));
        return this;
    }

    /**
     * Add an attribute of Double type
     * @param name
     * @param value
     * @return
     */
    public TimestreamMeta addAttribute(String name, double value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromDouble(value));
        return this;
    }

    /**
     * Add an attribute of Boolean type
     * @param name
     * @param value
     * @return
     */
    public TimestreamMeta addAttribute(String name, boolean value) {
        checkKeyParams(name);
        this.attributes.put(name, ColumnValue.fromBoolean(value));
        return this;
    }

    public Map<String, ColumnValue> getAttributes() {
        return this.attributes;
    }

    public String getAttributeAsString(String name) {
        return this.attributes.get(name).asString();
    }

    public long getAttributeAsLong(String name) {
        return this.attributes.get(name).asLong();
    }

    public byte[] getAttributeAsBinary(String name) {
        return this.attributes.get(name).asBinary();
    }

    public double getAttributeAsDouble(String name) {
        return this.attributes.get(name).asDouble();
    }

    public boolean getAttributeAsBoolean(String name) {
        return this.attributes.get(name).asBoolean();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.identifier.toString());
        for (String name : this.attributes.keySet()) {
            sb.append(", ");
            sb.append(name);
            sb.append("=");
            sb.append(this.attributes.get(name));
        }
        sb.append(", updateTime=").append(updateTime);
        return sb.toString();
    }
}
