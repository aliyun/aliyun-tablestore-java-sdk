package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

/**
 * The unit of a table's read/write throughput, namely the capacity unit.
 * Mainly used for configuring the reserved read/write throughput of a table in {@link ReservedThroughput} and for indicating the amount of capacity units consumed by read/write operations.
 */
public class CapacityUnit implements Jsonizable {
    /**
     * Read capacity unit.
     */
    private OptionalValue<Integer> readCapacityUnit = new OptionalValue<Integer>("ReadCapacityUnit");

    /**
     * Write capacity unit.
     */
    private OptionalValue<Integer> writeCapacityUnit = new OptionalValue<Integer>("WriteCapacityUnit");

    /**
     * Default constructor.
     * <p>Read capacity units and write capacity units are not set by default.</p>
     */
    public CapacityUnit() {
    }

    /**
     * Construct a CapacityUnit object and specify the values for read capacity units and write capacity units.
     *
     * @param readCapacityUnit  The value of the read capacity unit, which must be greater than or equal to 0.
     * @param writeCapacityUnit The value of the write capacity unit, which must be greater than or equal to 0.
     * @throws IllegalArgumentException If the value of the read or write capacity unit is negative.
     */
    public CapacityUnit(int readCapacityUnit, int writeCapacityUnit) {
        setReadCapacityUnit(readCapacityUnit);
        setWriteCapacityUnit(writeCapacityUnit);
    }

    /**
     * Get the value of the read capacity unit.
     *
     * @return The value of the read capacity unit.
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public int getReadCapacityUnit() {
        if (!readCapacityUnit.isValueSet()) {
            throw new IllegalStateException("The value of read capacity unit is not set.");
        }
        return readCapacityUnit.getValue();
    }

    /**
     * Set the value of the read capacity unit, the set value must be greater than or equal to 0.
     *
     * @param readCapacityUnit the value of the read capacity unit
     * @throws IllegalArgumentException if the value of the read capacity unit is negative.
     */
    public void setReadCapacityUnit(int readCapacityUnit) {
        Preconditions.checkArgument(readCapacityUnit >= 0, "The value of read capacity unit can't be negative.");
        this.readCapacityUnit.setValue(readCapacityUnit);
    }

    /**
     * Query whether the read capacity unit is set.
     *
     * @return Whether the read capacity unit is set
     */
    public boolean hasSetReadCapacityUnit() {
        return readCapacityUnit.isValueSet();
    }

    /**
     * Clear the set read CapacityUnit.
     */
    public void clearReadCapacityUnit() {
        readCapacityUnit.clear();
    }
    
    /**
     * Get the value of the write capacity unit.
     *
     * @return The value of the write capacity unit.
     * @throws java.lang.IllegalStateException If this parameter is not configured
     */
    public int getWriteCapacityUnit() {
        if (!writeCapacityUnit.isValueSet()) {
            throw new IllegalStateException("The value of write capacity unit is not set.");
        }
        return writeCapacityUnit.getValue();
    }

    /**
     * Set the value of the write capacity unit, the set value must be greater than or equal to 0.
     *
     * @param writeCapacityUnit the value of the write capacity unit
     * @throws IllegalArgumentException if the value of the write capacity unit is negative.
     */
    public void setWriteCapacityUnit(int writeCapacityUnit) {
        Preconditions.checkArgument(writeCapacityUnit >= 0, "The value of write capacity unit can't be negative.");
        this.writeCapacityUnit.setValue(writeCapacityUnit);
    }

    /**
     * Query whether the write capacity unit is set.
     *
     * @return whether the write capacity unit is set
     */
    public boolean hasSetWriteCapacityUnit() {
        return writeCapacityUnit.isValueSet();
    }

    /**
     * Clear the set write CapacityUnit.
     */
    public void clearWriteCapacityUnit() {
        writeCapacityUnit.clear();
    }

    @Override
    public int hashCode() {
        return readCapacityUnit.hashCode() ^ writeCapacityUnit.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof CapacityUnit)) {
            return false;
        }

        CapacityUnit c1 = (CapacityUnit) o;
        return this.readCapacityUnit.equals(c1.readCapacityUnit) && this.writeCapacityUnit.equals(c1.writeCapacityUnit);
    }

    @Override
    public String toString() {
        return "" + readCapacityUnit + ", " + writeCapacityUnit;
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
        if (readCapacityUnit.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Read\": ");
            sb.append(readCapacityUnit.getValue());
        }
        if (writeCapacityUnit.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"Write\": ");
            sb.append(writeCapacityUnit.getValue());
        }
        sb.append('}');
    }
}
