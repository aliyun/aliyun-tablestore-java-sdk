package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.*;

/**
 * In TableStore, each row of data can contain one or more attribute columns ({@link Column}),
 * and each attribute column includes a name, a value ({@link ColumnValue}), and a timestamp.
 */
public class Column implements Jsonizable, Measurable {
    public static NameTimestampComparator NAME_TIMESTAMP_COMPARATOR = new NameTimestampComparator();

    /**
     * The name of the attribute column.
     */
    private String name;
    /**
     * The value of the attribute column.
     */
    private ColumnValue value;

    /**
     * The timestamp of the attribute column.
     */
    private OptionalValue<Long> timestamp = new OptionalValue<Long>("Timestamp");

    /**
     * The data size occupied after serialization
     */
    private int dataSize = -1;

    /**
     * Constructs a property column, which must include a name, value, and timestamp.
     * <p>The name of the property column cannot be a null pointer or an empty string.</p>
     * <p>The value of the property column cannot be a null pointer.</p>
     *
     * @param name      The name of the property column
     * @param value     The value of the property column
     * @param timestamp The timestamp of the property column
     */
    public Column(String name, ColumnValue value, long timestamp) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of column should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of column should not be null.");
        Preconditions.checkArgument(timestamp >= 0, "The timestamp should not be negative.");

        this.name = name;
        this.value = value;
        this.timestamp.setValue(timestamp);
    }

    public Column(String name, ColumnValue value) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of column should not be null or empty.");
        Preconditions.checkNotNull(value, "The value of column should not be null.");

        this.name = name;
        this.value = value;
    }

    /**
     * Get the name of the attribute column.
     *
     * @return the name of the attribute column
     */
    public String getName() {
        return name;
    }

    public byte[] getNameRawData() {
        return Bytes.toBytes(name);
    }

    /**
     * Get the value of the attribute column.
     *
     * @return the value of the attribute column
     */
    public ColumnValue getValue() {
        return value;
    }

    /**
     * Get the timestamp of the attribute column.
     *
     * @return the timestamp of the attribute column
     * @throws java.lang.IllegalStateException if this parameter is not set
     */
    public long getTimestamp() {
        if (!timestamp.isValueSet()) {
            throw new IllegalStateException("The value of Timestamp is not set.");
        }
        return timestamp.getValue();
    }

    /**
     * Check if a timestamp has been set.
     *
     * @return Returns true if a timestamp has been set, otherwise returns false.
     */
    public boolean hasSetTimestamp() {
        return timestamp.isValueSet();
    }

    @Override
    public String toString() {
        return "Name:" + name + ",Value:" + value + "," + timestamp;
    }

    @Override
    public int hashCode() {
        return name.hashCode() ^ value.hashCode() ^ timestamp.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Column)) {
            return false;
        }

        Column col = (Column) o;
        return this.name.equals(col.name) && this.value.equals(col.value) && this.timestamp.equals(col.timestamp);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{\"Name\": \"");
        sb.append(name);
        sb.append("\", ");

        if (timestamp.isValueSet()) {
            sb.append("\"Timestamp\": ");
            sb.append(timestamp.getValue());
            sb.append(", ");
        }

        sb.append("\"Value\": ");
        value.jsonize(sb, newline + "  ");
        sb.append("}");
    }

    @Override
    public int getDataSize() {
        if (dataSize == -1) {
            int size = CalculateHelper.calcStringSizeInBytes(name) + value.getDataSize();
            if (hasSetTimestamp()) {
                size += 8;
            }
            dataSize = size;
        }
        return dataSize;
    }
}
