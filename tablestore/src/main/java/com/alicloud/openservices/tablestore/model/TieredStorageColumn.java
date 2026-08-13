package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Represents the properties of a custom time column used for tiered storage.
 * The time value of the custom column must be the time interval since 1970-01-01, and the unit can be configured.
 */
public class TieredStorageColumn implements Jsonizable {

    /**
     * The name of the custom time column.
     */
    private String name;

    /**
     * The time unit of the column's value. Defaults to TU_MILLISECOND.
     */
    private OptionalValue<TimeUnit> valueTimeUnit = new OptionalValue<TimeUnit>("ValueTimeUnit");

    /**
     * Construct a TieredStorageColumn instance.
     *
     * @param name The name of the custom time column.
     */
    public TieredStorageColumn(String name) {
        setName(name);
    }

    /**
     * Get the name of the custom time column.
     *
     * @return The name of the custom time column.
     */
    public String getName() {
        return name;
    }

    /**
     * Set the name of the custom time column.
     *
     * @param name The name of the custom time column.
     */
    public void setName(String name) {
        Preconditions.checkNotNull(name, "The column name should not be null.");
        Preconditions.checkArgument(!name.isEmpty(), "The column name should not be empty.");
        this.name = name;
    }

    /**
     * Get the time unit of the column's value.
     *
     * @return The time unit of the column's value, or null if not set.
     */
    public TimeUnit getValueTimeUnit() {
        return valueTimeUnit.getValue();
    }

    /**
     * Set the time unit of the column's value.
     *
     * @param valueTimeUnit The time unit of the column's value.
     */
    public void setValueTimeUnit(TimeUnit valueTimeUnit) {
        this.valueTimeUnit.setValue(valueTimeUnit);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"Name\": \"");
        sb.append(name);
        sb.append("\"");
        if (valueTimeUnit.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"ValueTimeUnit\": \"");
            sb.append(valueTimeUnit.getValue().toString());
            sb.append("\"");
        }
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Name: ").append(name);
        if (valueTimeUnit.isValueSet()) {
            sb.append(", ValueTimeUnit: ").append(valueTimeUnit.getValue().toString());
        }
        return sb.toString();
    }
}
