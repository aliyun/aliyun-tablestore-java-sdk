package com.aliyun.openservices.ots.model;

public class DeleteRowResult extends OTSResult {
    private ConsumedCapacity consumedCapacity;

    public DeleteRowResult(OTSResult meta) {
        super(meta);
    }

    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }
}
