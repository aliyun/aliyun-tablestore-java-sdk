package com.alicloud.openservices.tablestore.model;

public class ReservedThroughput {
    
    /**
     * The reserved throughput configuration for the table.
     */
    private CapacityUnit capacityUnit;

    /**
     * Initializes {@link ReservedThroughput} with the default reserved read/write throughput configuration (0 units of read capacity and 0 units of write capacity).
     */
    public ReservedThroughput() {
        capacityUnit = new CapacityUnit(0, 0);
    }

    /**
     * Initialize {@link ReservedThroughput}.
     * @param capacityUnit The reserved read/write throughput setting for the table.
     */
    public ReservedThroughput(CapacityUnit capacityUnit) {
        setCapacityUnit(capacityUnit);
    }

    /**
     * Initialize {@link ReservedThroughput}.
     * @param read The configuration of the table's reserved read throughput.
     * @param write The configuration of the table's reserved write throughput.
     */
    public ReservedThroughput(int read, int write) {
        setCapacityUnit(new CapacityUnit(read, write));
    }

    /**
     * Get the value of the table's reserved throughput.
     * @return CapacityUnit.
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * Sets the reserved throughput value for the table; you must set both the read and write capacity units simultaneously.
     * @param capacityUnit The capacity unit to be set.
     */
    public void setCapacityUnit(CapacityUnit capacityUnit) {
        this.capacityUnit = capacityUnit;
    }

    @Override
    public int hashCode() {
        return this.capacityUnit.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ReservedThroughput)){
            return false;
        }

        ReservedThroughput r1 = (ReservedThroughput)o;
        return this.capacityUnit.equals(r1.capacityUnit);
    }
}
