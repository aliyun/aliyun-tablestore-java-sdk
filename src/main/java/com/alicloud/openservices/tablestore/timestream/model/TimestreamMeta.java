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
 * 时间线，包含：
 * 1. {@link TimestreamIdentifier}：时间线唯一标示
 * 2. updateTime(更新时间)：时间线的更新时间，数据写入时后台会自动更新该时间
 * 3. attributes：时间线的可变属性，属性变化时并不影响时间线的确认
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
     * attribute的key不能为保留字段
     * @param key
     */
    private void checkKeyParams(String key) {
        if (key.equals(TableMetaGenerator.CN_TAMESTAMP_NAME)) {
            throw new ClientException("Key of attribute cannot be " + TableMetaGenerator.CN_TAMESTAMP_NAME + ".");
        }
    }

    /**
     * 设置attributes
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
     * 添加一个String类型的attribute
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
     * 添加一个Long类型的attribute
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
     * 添加一个Binary类型的attribute
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
     * 添加一个Double类型的attribute
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
     * 添加一个Boolean类型的attribute
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
