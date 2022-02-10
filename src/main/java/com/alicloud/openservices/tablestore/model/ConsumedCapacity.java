package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 读写操作消耗的能力单元和数据大小。
 * <p>读操作会消耗读能力单元，例如GetRow、GetRange和BatchGetRow等。</p>
 * <p>写操作会消耗写能力单元，例如PutRow、UpdateRow、DeleteRow和BatchWriteRow等。</p>
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
     * 返回消耗的能力单元的值。
     *
     * @return 能力单元
     */
    public CapacityUnit getCapacityUnit() {
        return capacityUnit;
    }

    /**
     * 返回消耗的数据大小的值。
     *
     * @return 能力单元
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
