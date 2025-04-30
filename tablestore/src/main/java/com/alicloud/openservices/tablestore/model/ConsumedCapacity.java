package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * The capacity unit and data size consumed by read and write operations.
 * <p>Read operations consume read capacity units, such as GetRow, GetRange, and BatchGetRow.</p>
 * <p>Write operations consume write capacity units, such as PutRow, UpdateRow, DeleteRow, and BatchWriteRow.</p>
 */
public class ConsumedCapacity implements Jsonizable {

    private CapacityUnit capacityUnit = null;
    private CapacityDataSize capacityDataSize = null;

    public ConsumedCapacity(CapacityUnit capacityUnit) {
        Preconditions.checkNotNull(capacityUnit);
        this.capacityUnit = capacityUnit;
    }

    public ConsumedCapacity(CapacityDataSize capacityDataSize) {
        Preconditions.checkNotNull(capacityDataSize);
        this.capacityDataSize = capacityDataSize;
    }

    public ConsumedCapacity(CapacityUnit capacityUnit, CapacityDataSize capacityDataSize) {
        Preconditions.checkNotNull(capacityUnit);
        this.capacityUnit = capacityUnit;

        Preconditions.checkNotNull(capacityDataSize);
        this.capacityDataSize = capacityDataSize;
    }

    public void setCapacityUnit(CapacityUnit capacityUnit){
        Preconditions.checkNotNull(capacityUnit);
        this.capacityUnit = capacityUnit;
    }

    public void setCapacityDataSize(CapacityDataSize  capacityDataSize){
        Preconditions.checkNotNull(capacityDataSize);
        this.capacityDataSize = capacityDataSize;
    }
    /**
     * Returns the consumed capacity unit.
     *
     * @return Capacity Unit
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * Returns the value of the consumed data size.
     *
     * @return capacity unit
     */
    public CapacityDataSize getCapacityDataSize() {
        return capacityDataSize;
    }

    @Override
    public String jsonize() {
        return capacityUnit.jsonize();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        capacityUnit.jsonize(sb, newline);
    }
}
