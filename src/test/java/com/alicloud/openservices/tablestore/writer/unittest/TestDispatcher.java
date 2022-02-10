package com.alicloud.openservices.tablestore.writer.unittest;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.dispatch.HashPartitionKeyDispatcher;
import com.alicloud.openservices.tablestore.writer.dispatch.HashPrimaryKeyDispatcher;
import com.alicloud.openservices.tablestore.writer.dispatch.RoundRobinDispatcher;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;


public class TestDispatcher {
    static final int BUCKET_COUNT = (int) (Math.random() * 100);
    static final int TOTAL_PRIMARYKEY = 1000000;

    @Test
    public void testHashPrimaryKeyDispatcher() {

        HashPrimaryKeyDispatcher dispatcher = new HashPrimaryKeyDispatcher(BUCKET_COUNT);
        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_PRIMARYKEY; i++) {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("String_" + i))
                    .build();
            RowChange rowChange = new RowPutChange("name", primaryKey);
            dispatcher.getDispatchIndex(rowChange);
        }
        System.out.println("HashPrimaryKeyDispatcher Cost: " + (System.currentTimeMillis() - start));


        System.out.println("Bucket Count: " + Arrays.asList(dispatcher.getBucketDispatchRowCount()));
        Assert.assertEquals(dispatcher.getBucketDispatchRowCount().length, BUCKET_COUNT);

        int total = 0;
        for (AtomicLong bucket : dispatcher.getBucketDispatchRowCount()) {
            total += bucket.get();
        }
        Assert.assertEquals(total, TOTAL_PRIMARYKEY);
    }

    @Test
    public void testHashPartitionKeyDispatcher() {

        HashPartitionKeyDispatcher dispatcher = new HashPartitionKeyDispatcher(BUCKET_COUNT);
        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_PRIMARYKEY; i++) {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("String_" + i))
                    .build();
            RowChange rowChange = new RowPutChange("name", primaryKey);
            dispatcher.getDispatchIndex(rowChange);
        }
        System.out.println("HashPartitionKeyDispatcher Cost: " + (System.currentTimeMillis() - start));


        System.out.println("Bucket Count: " + Arrays.asList(dispatcher.getBucketDispatchRowCount()));
        Assert.assertEquals(dispatcher.getBucketDispatchRowCount().length, BUCKET_COUNT);

        int total = 0;
        for (AtomicLong bucket : dispatcher.getBucketDispatchRowCount()) {
            total += bucket.get();
        }
        Assert.assertEquals(total, TOTAL_PRIMARYKEY);
    }


    @Test
    public void testLoopDispatcher() {

        RoundRobinDispatcher dispatcher = new RoundRobinDispatcher(BUCKET_COUNT);
        long start = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_PRIMARYKEY; i++) {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(i))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("String_" + i))
                    .build();
            RowChange rowChange = new RowPutChange("name", primaryKey);
            dispatcher.getDispatchIndex(rowChange);
        }
        System.out.println("RoundRobinDispatcher Cost: " + (System.currentTimeMillis() - start));


        System.out.println("Bucket Count: " + Arrays.asList(dispatcher.getBucketDispatchRowCount()));
        Assert.assertEquals(dispatcher.getBucketDispatchRowCount().length, BUCKET_COUNT);

        int total = 0;
        for (AtomicLong bucket : dispatcher.getBucketDispatchRowCount()) {
            total += bucket.get();
        }
        Assert.assertEquals(total, TOTAL_PRIMARYKEY);
    }

}
