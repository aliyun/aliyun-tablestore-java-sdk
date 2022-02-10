package com.alicloud.openservices.tablestore.writer.dispatch;

import com.alicloud.openservices.tablestore.model.*;


public class HashPrimaryKeyDispatcher extends BaseDispatcher {
    private int bucketCount;

    public HashPrimaryKeyDispatcher(int bucketCount) {
        super(bucketCount);
        this.bucketCount = bucketCount;
    }

    @Override
    public int getDispatchIndex(RowChange rowChange) {
        int bucketIndex = rowChange.getPrimaryKey().hashCode() % bucketCount;
        bucketIndex = (bucketIndex + bucketCount) % bucketCount;
        addBucketCount(bucketIndex);

        return bucketIndex;
    }
}
