package com.alicloud.openservices.tablestore.writer.dispatch;

import com.alicloud.openservices.tablestore.model.*;


public class HashPartitionKeyDispatcher extends BaseDispatcher {
    private int bucketCount;

    public HashPartitionKeyDispatcher(int bucketCount) {
        super(bucketCount);
        this.bucketCount = bucketCount;
    }

    @Override
    public int getDispatchIndex(RowChange rowChange) {
        int bucketIndex = rowChange.getPrimaryKey().getPrimaryKeyColumn(0).hashCode() % bucketCount;
        bucketIndex = (bucketIndex + bucketCount) % bucketCount;
        addBucketCount(bucketIndex);

        return bucketIndex;
    }

}
