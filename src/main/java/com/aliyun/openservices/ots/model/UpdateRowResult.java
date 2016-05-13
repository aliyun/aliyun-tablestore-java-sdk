package com.aliyun.openservices.ots.model;

public class UpdateRowResult extends OTSResult {
    /**
     * 此次操作消耗的CapacityUnit。
     */
    private ConsumedCapacity consumedCapacity;
    
    public UpdateRowResult() {
        this.consumedCapacity = new ConsumedCapacity();
    }
    
    public UpdateRowResult(OTSResult meta) {
        super(meta);
        this.consumedCapacity = new ConsumedCapacity();
    }

    /**
     * 获取此次操作消耗的CapacityUnit。
     * @return 此次操作消耗的CapacityUnit。
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }
}
