package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowChange;

public class ReaderDispatcher {
    private int bucketCount;
    private final AtomicLong[] bucketDispatchRowCount;

    public ReaderDispatcher(int bucketCount) {
        bucketDispatchRowCount = new AtomicLong[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            bucketDispatchRowCount[i] = new AtomicLong(0);
        }
        this.bucketCount = bucketCount;
    }

    public int getDispatchIndex(PrimaryKey primaryKey) {
        int bucketIndex = primaryKey.hashCode() % bucketCount;
        bucketIndex = (bucketIndex + bucketCount) % bucketCount;
        addBucketCount(bucketIndex);

        return bucketIndex;
    }

    private void addBucketCount(int bucketId) {
        bucketDispatchRowCount[bucketId].incrementAndGet();
    }
}
