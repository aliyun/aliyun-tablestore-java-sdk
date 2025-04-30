package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * The unit of read/write throughput for a table, also known as a capacity unit.
 * Primarily used in {@link ReservedThroughput} to configure the reserved read/write throughput for a table and to indicate the amount of capacity units consumed by read/write operations.
 */
public class CapacityDataSize implements Jsonizable {
    /**
     * Read capacity unit.
     */
    private OptionalValue<Long> readCapacityDataSize = new OptionalValue<Long>("ReadCapacityDataSize");

    /**
     * Write capacity unit.
     */
    private OptionalValue<Long> writeCapacityDataSize = new OptionalValue<Long>("WriteCapacityDataSize");

    /**
     * Default constructor.
     * <p>Read capacity units and write capacity units are not set by default.</p>
     */
    public CapacityDataSize() {
    }

    /**
     * Constructs a CapacityUnit object and specifies the values for read capacity units and write capacity units.
     *
     * @param readCapacityDataSize  The value of the read capacity unit, which must be greater than or equal to 0.
     * @param writeCapacityDataSize The value of the write capacity unit, which must be greater than or equal to 0.
     * @throws IllegalArgumentException If the read or write capacity unit value is negative.
     */
    public CapacityDataSize(long readCapacityDataSize, long writeCapacityDataSize) {
        setReadCapacityDataSize(readCapacityDataSize);
        setWriteCapacityDataSize(writeCapacityDataSize);
    }
    /**
     * Get the value of the read capacity unit.
     *
     * @return The value of the read capacity unit.
     * @throws java.lang.IllegalStateException if this parameter is not configured.
     */
    public long getReadCapacityDataSize() {
        if (!readCapacityDataSize.isValueSet()) {
            throw new IllegalStateException("The value of read capacity unit is not set.");
        }
        return readCapacityDataSize.getValue();
    }

    /**
     * Set the value of the read capacity unit. The set value must be greater than or equal to 0.
     *
     * @param readCapacityDataSize the value of the read capacity unit
     * @throws IllegalArgumentException if the value of the read capacity unit is negative.
     */
    public void setReadCapacityDataSize(long readCapacityDataSize) {
        Preconditions.checkArgument(readCapacityDataSize >= 0, "The value of read capacity DataSize can't be negative.");
        this.readCapacityDataSize.setValue(readCapacityDataSize);
    }

    /**
     * Query whether the read capacity unit is set.
     *
     * @return Whether the read capacity unit is set
     */
    public boolean hasSetReadCapacityDataSize() {
        return readCapacityDataSize.isValueSet();
    }

    /**
     * Clear the set read CapacityDataSize.
     */
    public void clearReadCapacityDataSize() {
        readCapacityDataSize.clear();
    }

    /**
     * Get the value of the write capacity unit.
     *
     * @return The value of the write capacity unit.
     * @throws java.lang.IllegalStateException If this parameter is not configured
     */
    public long getWriteCapacityDataSize() {
        if (!writeCapacityDataSize.isValueSet()) {
            throw new IllegalStateException("The value of write capacity unit is not set.");
        }
        return writeCapacityDataSize.getValue();
    }

    /**
     * Set the value of the write capacity unit. The set value must be greater than or equal to 0.
     *
     * @param writeCapacityDataSize the value of the write capacity unit
     * @throws IllegalArgumentException if the value of the write capacity unit is negative.
     */
    public void setWriteCapacityDataSize(long writeCapacityDataSize) {
        Preconditions.checkArgument(writeCapacityDataSize >= 0, "The value of write capacity unit can't be negative.");
        this.writeCapacityDataSize.setValue(writeCapacityDataSize);
    }

    /**
     * Query whether the write capacity unit is set.
     *
     * @return whether the write capacity unit is set
     */
    public boolean hasSetWriteCapacityDataSize() {
        return writeCapacityDataSize.isValueSet();
    }

    /**
     * Clear the set write CapacityUnit.
     */
    public void clearWriteCapacityDataSize() {
        writeCapacityDataSize.clear();
    }

    @Override
    public int hashCode() {
        return readCapacityDataSize.hashCode() ^ writeCapacityDataSize.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof CapacityDataSize)) {
            return false;
        }

        CapacityDataSize c1 = (CapacityDataSize) o;
        return this.readCapacityDataSize.equals(c1.readCapacityDataSize) && this.writeCapacityDataSize.equals(c1.writeCapacityDataSize);
    }

    @Override
    public String toString() {
        return "" + readCapacityDataSize + ", " + writeCapacityDataSize;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        boolean firstItem = true;
        sb.append('{');
        if (readCapacityDataSize.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Read_Size\": ");
            sb.append(readCapacityDataSize.getValue());
        }
        if (writeCapacityDataSize.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Write_Size\": ");
            sb.append(writeCapacityDataSize.getValue());
        }
        sb.append('}');
    }
}