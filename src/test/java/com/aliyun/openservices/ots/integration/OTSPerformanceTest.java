/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.integration;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.GetRowRequest;
import com.aliyun.openservices.ots.model.GetRowResult;
import com.aliyun.openservices.ots.model.MultiRowQueryCriteria;
import com.aliyun.openservices.ots.model.OTSContext;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.PutRowResult;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.ReservedThroughputChange;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;
import com.aliyun.openservices.ots.model.SingleRowQueryCriteria;
import com.aliyun.openservices.ots.model.TableMeta;
import com.aliyun.openservices.ots.model.UpdateTableRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus;
import com.aliyun.openservices.ots.utils.ServiceSettings;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * 测试环境为24核机器。
 */
public class OTSPerformanceTest {

    private final static int ROW_COUNT = 20000000;
    private static long[] latency = new long[ROW_COUNT];
    private static String tableName = "java_sdk_perf_test";
    private static int MILLISECONDS_UNTIL_TABLE_READY = 30 * 1000;
    private static AtomicLong errCount = new AtomicLong();

    public static void main(String[] args) throws InterruptedException {
//         testPutRowAsync();
         testGetRowAsync();
//         testGetRangeAsync();
//         testBatchWriteRowAsync();
//         testBatchGetRowAsync();
//         testPutRow();
//         testGetRow();
//         testGetRange();
//         testBatchWriteRow();
//         testBatchGetRow();
//        test();
    }

    private static List<OTSAsync> getOTSAsyncList(int clientCount,
            int ioThreadCountPerClient, int connectionsPerClient) {
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(connectionsPerClient);
        config.setIoThreadCount(ioThreadCountPerClient);
        config.setSocketTimeoutInMillisecond(8000);
        OTSServiceConfiguration serviceConfig = new OTSServiceConfiguration();
        serviceConfig.setEnableResponseValidation(false);
        List<OTSAsync> otsList = new ArrayList<OTSAsync>();
        for (int i = 0; i < clientCount; i++) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            OTSAsync ots = OTSClientFactory.createOTSClientAsync(
                    ServiceSettings.load(), config, serviceConfig, pool);
            otsList.add(ots);
        }
        return otsList;
    }

    private static List<OTS> getOTSList(int clientCount,
            int ioThreadCountPerClient, int connectionsPerClient) {
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(connectionsPerClient);
        config.setIoThreadCount(ioThreadCountPerClient);
        config.setSocketTimeoutInMillisecond(10000);
        OTSServiceConfiguration serviceConfig = new OTSServiceConfiguration();
        serviceConfig.setEnableResponseValidation(false);
        List<OTS> otsList = new ArrayList<OTS>();
        for (int i = 0; i < clientCount; i++) {
            OTS ots = OTSClientFactory.createOTSClient(ServiceSettings.load(),
                    config, serviceConfig);
            otsList.add(ots);
        }
        return otsList;
    }
    
    public static void test() throws InterruptedException {
        errCount.set(0);
        int clientCount = 1;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 6, 30);
        int producerThreadCount = 1;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 200) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        putRowAsync(ots, i, latch);
                        if (errCount.get() > 0) {
                            break;
                        }
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test GetRowAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }
    
    public static void testGetRowAsync() throws InterruptedException {
        errCount.set(0);
        int clientCount = 9;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 4, 100);
        int producerThreadCount = 36;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 1200) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        getRowAsync(ots, i, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test GetRowAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    public static void testPutRowAsync() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 6, 200);
        int producerThreadCount = 8;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 1500) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        putRowAsync(ots, i, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test PutRowAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    public static void testGetRangeAsync() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 6, 200);
        int producerThreadCount = 20;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        int tmp = (i % 61439) + 4096; // [4096, 65534]
                        String startPk = Integer.toHexString(tmp);
                        String endPk = Integer.toHexString(tmp + 1);
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 800) {
                            try {
                                Thread.sleep(5);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        getRangeAsync(ots, i, startPk, endPk, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test GetRangeAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    private static void testBatchGetRowAsync() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 6, 300);
        int producerThreadCount = 20;
        final int rowsPerRequest = 10;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread / rowsPerRequest
                * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i += rowsPerRequest) {
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 1000) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        batchGetRowAsync(ots, i, rowsPerRequest, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test BatchGetRowAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    private static void testBatchWriteRowAsync() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTSAsync> otsList = getOTSAsyncList(clientCount, 6, 100);
        int producerThreadCount = 20;
        final int rowsPerRequest = 10;
        int rowPerThread = ROW_COUNT / producerThreadCount;
        final int totalRequest = rowPerThread / rowsPerRequest
                * producerThreadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        final AtomicLong sendCount = new AtomicLong();
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < producerThreadCount; i++) {
            final OTSAsync ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i += rowsPerRequest) {
                        long send = sendCount.incrementAndGet();
                        if (totalRequest - latch.getCount() < send - 300) {
                            try {
                                Thread.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        batchWriteRowAsync(ots, i, rowsPerRequest, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test BatchWriteRowAsync:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    public static void testGetRow() throws InterruptedException {
        errCount.set(0);
        int clientCount = 1;
        List<OTS> otsList = getOTSList(clientCount, 24, 100);
        int threadCount = 100;
        int rowPerThread = ROW_COUNT / threadCount;
        int totalRequest = rowPerThread * threadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final OTS ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        long timeSend = System.currentTimeMillis();
                        getRow(ots, i, latch);
                        if (errCount.get() > 0) {
                            System.out.println(timeSend);
                            break;
                        }
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test GetRow:");
        Summary(ROW_COUNT, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    public static void testPutRow() throws InterruptedException {
        int clientCount = 4;
        List<OTS> otsList = getOTSList(clientCount, 6, 150);
        int threadCount = 400;
        int rowPerThread = ROW_COUNT / threadCount;
        int totalRequest = rowPerThread * threadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final OTS ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        putRow(ots, i, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test PutRow:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    public static void testGetRange() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTS> otsList = getOTSList(clientCount, 6, 150);
        int threadCount = 400;
        int rowPerThread = ROW_COUNT / threadCount;
        final int totalRequest = rowPerThread * threadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final OTS ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i++) {
                        int tmp = (i % 61439) + 4096; // [4096, 65534]
                        String startPk = Integer.toHexString(tmp);
                        String endPk = Integer.toHexString(tmp + 1);
                        getRange(ots, i, startPk, endPk, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test GetRange:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    private static void testBatchGetRow() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTS> otsList = getOTSList(clientCount, 6, 150);
        int threadCount = 400;
        final int rowsPerRequest = 10;
        int rowPerThread = ROW_COUNT / threadCount;
        int totalRequest = rowPerThread / rowsPerRequest * threadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final OTS ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i += rowsPerRequest) {
                        batchGetRow(ots, i, rowsPerRequest, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test BatchGetRow:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    private static void testBatchWriteRow() throws InterruptedException {
        errCount.set(0);
        int clientCount = 4;
        List<OTS> otsList = getOTSList(clientCount, 6, 150);
        int threadCount = 400;
        final int rowsPerRequest = 10;
        int rowPerThread = ROW_COUNT / threadCount;
        int totalRequest = rowPerThread / rowsPerRequest * threadCount;
        final CountDownLatch latch = new CountDownLatch(totalRequest);
        long time0 = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            final OTS ots = otsList.get(i % clientCount);
            final int start = i * rowPerThread;
            final int end = start + rowPerThread;
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    for (int i = start; i < end; i += rowsPerRequest) {
                        batchWriteRow(ots, i, rowsPerRequest, latch);
                    }
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
        }
        latch.await();
        long time1 = System.currentTimeMillis();
        System.out.println("Test BatchWriteRow:");
        Summary(totalRequest, time1 - time0);
        for (int i = 0; i < clientCount; i++) {
            otsList.get(i).shutdown();
        }
    }

    private static void Summary(int totalReq, long timeSpan) {
        long latencySum = 0;
        Arrays.sort(latency, 0, totalReq);
        for (int i = 0; i < totalReq; i++) {
            latencySum += latency[i];
        }
        double latencyAver = latencySum / totalReq;
        System.out.println("Total Request:" + totalReq);
        System.out.println("Error Count:" + errCount.get());
        System.out.println("qps:" + (totalReq * (long) 1000 / timeSpan));
        System.out.println("average latency: " + latencyAver);
        System.out.println("Top 1 latency: " + latency[totalReq - 1]);
        System.out.println("Top " + totalReq / 100 + " latency: "
                + latency[totalReq - totalReq / 100]);
        System.out.println("Top " + totalReq / 10 + " latency: "
                + latency[totalReq - totalReq / 10]);
        System.out.println("Top " + totalReq / 5 + " latency: "
                + latency[totalReq - totalReq / 5]);
        System.out.println("Top " + totalReq / 2 + " latency: "
                + latency[totalReq - totalReq / 2]);
        System.out.println();
    }

    private static void getRow(OTS ots, int pk, CountDownLatch latch) {
        GetRowRequest request = new GetRowRequest();
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1",
                PrimaryKeyValue.fromString(toHexAndReverse(pk)));
        pks.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("" + pk));
        pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
        rowQueryCriteria.setPrimaryKey(pks);
        String[] columnNames = new String[10];
        for (int i = 1; i <= 10; i++) {
            columnNames[i - 1] = "col" + i;
        }
        rowQueryCriteria.addColumnsToGet(columnNames);
        request.setRowQueryCriteria(rowQueryCriteria);
        try {
            long timeSend = System.currentTimeMillis();
            ots.getRow(request);
            latency[pk] = System.currentTimeMillis() - timeSend;
            if (latch != null) {
                latch.countDown();
            }
        } catch (ClientException ex) {
            System.out.println(ex.getCause());
            ex.printStackTrace();
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private static void getRowAsync(OTSAsync ots, final int pk,
            final CountDownLatch latch) {
        GetRowRequest request = new GetRowRequest();
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1",
                PrimaryKeyValue.fromString(toHexAndReverse(pk)));
        pks.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("" + pk));
        pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
        rowQueryCriteria.setPrimaryKey(pks);
        String[] columnNames = new String[10];
        for (int i = 1; i <= 10; i++) {
            columnNames[i - 1] = "col" + i;
        }
        rowQueryCriteria.addColumnsToGet(columnNames);
        request.setRowQueryCriteria(rowQueryCriteria);
        final long timeSend = System.currentTimeMillis();
        ots.getRow(request, new OTSCallback<GetRowRequest, GetRowResult>() {

            @Override
            public void onCompleted(
                    OTSContext<GetRowRequest, GetRowResult> otsContext) {
                Row row = otsContext.getOTSResult().getRow();
                for (int i = 1; i <= 10; i++) {
                    String expect = "helloworld" + pk + i;
                    ColumnValue columnValue = row.getColumns().get("col" + i);
                    if (columnValue == null) {
                        System.out.println("pk:" + pk + ". Column col" + i + " does not exist.");
                        continue;
                    }
                    String value = columnValue.asString();
                    if (!value.equals(expect)) {
                        errCount.incrementAndGet();
                        System.out.println("Data Error!");
                    }
                }
                latency[pk] = System.currentTimeMillis() - timeSend;
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public void onFailed(
                    OTSContext<GetRowRequest, GetRowResult> otsContext,
                    OTSException ex) {
                System.out.println(ex.getErrorCode());
                ex.printStackTrace();
                errCount.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public void onFailed(
                    OTSContext<GetRowRequest, GetRowResult> otsContext,
                    ClientException ex) {
                System.out.println(ex.getMessage());
                System.out.println(timeSend);
                System.out.println(System.currentTimeMillis());
                errCount.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }
        });
    }

    private static void putRow(OTS ots, int pk, CountDownLatch latch) {
        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1",
                PrimaryKeyValue.fromString(toHexAndReverse(pk)));
        pks.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("" + pk));
        pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
        rowChange.setPrimaryKey(pks);
        for (int i = 1; i <= 10; i++) {
            rowChange.addAttributeColumn("col" + i,
                    ColumnValue.fromString("helloworld" + pk + i));
        }
        request.setRowChange(rowChange);
        try {
            long timeSend = System.currentTimeMillis();
            ots.putRow(request);
            latency[pk] = System.currentTimeMillis() - timeSend;
            if (latch != null) {
                latch.countDown();
            }
        } catch (ClientException ex) {
            System.out.println(ex.getCause());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private static void putRowAsync(OTSAsync ots, final int pk,
            final CountDownLatch latch) {
        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1",
                PrimaryKeyValue.fromString(toHexAndReverse(pk)));
        pks.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("" + pk));
        pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
        rowChange.setPrimaryKey(pks);
        for (int i = 1; i <= 10; i++) {
            rowChange.addAttributeColumn("col" + i,
                    ColumnValue.fromString("helloworld" + pk + i));
        }
        request.setRowChange(rowChange);
        final long timeSend = System.currentTimeMillis();
        ots.putRow(request, new OTSCallback<PutRowRequest, PutRowResult>() {
            @Override
            public void onCompleted(
                    OTSContext<PutRowRequest, PutRowResult> otsContext) {
                latency[pk] = System.currentTimeMillis() - timeSend;
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public void onFailed(
                    OTSContext<PutRowRequest, PutRowResult> otsContext,
                    OTSException ex) {
                System.out.println(ex.getErrorCode());
                errCount.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }

            @Override
            public void onFailed(
                    OTSContext<PutRowRequest, PutRowResult> otsContext,
                    ClientException ex) {
                System.out.println(ex.getCause());
                errCount.incrementAndGet();
                if (latch != null) {
                    latch.countDown();
                }
            }
        });
    }

    private static void getRange(OTS ots, int id, String startPk, String endPk,
            CountDownLatch latch) {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(
                tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        RowPrimaryKey end = new RowPrimaryKey();
        start.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(startPk));
        start.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        start.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(endPk));
        end.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(start);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
        rangeRowQueryCriteria.setLimit(10);
        rangeRowQueryCriteria
                .addColumnsToGet(new String[] { "col1", "col2", "col3", "col4",
                        "col5", "col6", "col7", "col8", "col9", "col10" });
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        try {
            long timeSend = System.currentTimeMillis();
            ots.getRange(getRangeRequest);
            latency[id] = System.currentTimeMillis() - timeSend;
            if (latch != null) {
                latch.countDown();
            }
        } catch (ClientException ex) {
            System.out.println(ex.getCause());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private static void getRangeAsync(OTSAsync ots, final int id,
            String startPk, String endPk, final CountDownLatch latch) {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(
                tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        RowPrimaryKey end = new RowPrimaryKey();
        start.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(startPk));
        start.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        start.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(endPk));
        end.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        end.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(start);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);
        rangeRowQueryCriteria.setLimit(10);
        rangeRowQueryCriteria
                .addColumnsToGet(new String[] { "col1", "col2", "col3", "col4",
                        "col5", "col6", "col7", "col8", "col9", "col10" });
        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        final long timeSend = System.currentTimeMillis();
        ots.getRange(getRangeRequest,
                new OTSCallback<GetRangeRequest, GetRangeResult>() {

                    @Override
                    public void onCompleted(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext) {

                        latency[id] = System.currentTimeMillis() - timeSend;
                        if (latch != null) {
                            latch.countDown();
                        }

                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext,
                            OTSException ex) {
                        System.out.println(ex.getErrorCode());
                        // ex.printStackTrace();
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext,
                            ClientException ex) {
                        System.out.println(ex.getCause());
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
                });
    }

    private static void batchGetRow(OTS ots, int start, int num,
            CountDownLatch latch) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        String[] columnNames = new String[10];
        for (int i = 1; i <= 10; i++) {
            columnNames[i - 1] = "col" + i;
        }
        criteria.addColumnsToGet(columnNames);
        for (int pk = start; pk < start + num; pk++) {
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("pk1",
                    PrimaryKeyValue.fromString(toHexAndReverse(start)));
            pks.addPrimaryKeyColumn("pk2",
                    PrimaryKeyValue.fromString("" + start));
            pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
            criteria.addRow(pks);
        }
        batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        int id = start / num;
        try {
            long timeSend = System.currentTimeMillis();
            ots.batchGetRow(batchGetRowRequest);
            latency[id] = System.currentTimeMillis() - timeSend;
            if (latch != null) {
                latch.countDown();
            }
        } catch (ClientException ex) {
            System.out.println(ex.getCause());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private static void batchGetRowAsync(OTSAsync ots, int start, int num,
            final CountDownLatch latch) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        String[] columnNames = new String[10];
        for (int i = 1; i <= 10; i++) {
            columnNames[i - 1] = "col" + i;
        }
        criteria.addColumnsToGet(columnNames);
        for (int pk = start; pk < start + num; pk++) {
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("pk1",
                    PrimaryKeyValue.fromString(toHexAndReverse(pk)));
            pks.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("" + pk));
            pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
            criteria.addRow(pks);
        }
        batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        final long timeSend = System.currentTimeMillis();
        final int id = start / num;
        ots.batchGetRow(batchGetRowRequest,
                new OTSCallback<BatchGetRowRequest, BatchGetRowResult>() {

                    @Override
                    public void onCompleted(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext) {
                        BatchGetRowResult result = otsContext.getOTSResult();
                        List<RowStatus> statues = result
                                .getBatchGetRowStatus(tableName);
                        for (RowStatus status : statues) {
                            if (!status.isSucceed()) {
                                System.out.println(status.getError()
                                        .getMessage());
                            }
                        }
                        latency[id] = System.currentTimeMillis() - timeSend;
                        if (latch != null) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext,
                            OTSException ex) {
                        System.out.println(ex.getErrorCode());
                        ex.printStackTrace();
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext,
                            ClientException ex) {
                        System.out.println(ex.getCause());
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
                });
    }

    private static void batchWriteRow(OTS ots, int start, int num,
            CountDownLatch latch) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int pk = start; pk < start + num; pk++) {
            // RowPutChange rowPutChange = new RowPutChange(tableName);
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName);
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("pk1",
                    PrimaryKeyValue.fromString(toHexAndReverse(start)));
            pks.addPrimaryKeyColumn("pk2",
                    PrimaryKeyValue.fromString("" + start));
            pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
            // rowPutChange.setPrimaryKey(pks);
            rowUpdateChange.setPrimaryKey(pks);
            for (int i = 1; i <= 10; i++) {
                // rowPutChange.addAttributeColumn("col" + i,
                // ColumnValue.fromString("helloworld" + pk + i));
                rowUpdateChange.addAttributeColumn("col" + i,
                        ColumnValue.fromString("helloworld" + pk + i));
            }
            batchWriteRowRequest.addRowUpdateChange(rowUpdateChange);
            // batchWriteRowRequest.addRowPutChange(rowPutChange);
        }
        int id = start / num;
        try {
            long timeSend = System.currentTimeMillis();
            ots.batchWriteRow(batchWriteRowRequest);
            latency[id] = System.currentTimeMillis() - timeSend;
            if (latch != null) {
                latch.countDown();
            }
        } catch (ClientException ex) {
            System.out.println(ex.getCause());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            errCount.incrementAndGet();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    private static void batchWriteRowAsync(OTSAsync ots, int start, int num,
            final CountDownLatch latch) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        for (int pk = start; pk < start + num; pk++) {
            // RowPutChange rowPutChange = new RowPutChange(tableName);
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName);
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("pk1",
                    PrimaryKeyValue.fromString(toHexAndReverse(start)));
            pks.addPrimaryKeyColumn("pk2",
                    PrimaryKeyValue.fromString("" + start));
            pks.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(pk));
            // rowPutChange.setPrimaryKey(pks);
            rowUpdateChange.setPrimaryKey(pks);
            for (int i = 1; i <= 10; i++) {
                // rowPutChange.addAttributeColumn("col" + i,
                // ColumnValue.fromString("helloworld" + pk + i));
                rowUpdateChange.addAttributeColumn("col" + i,
                        ColumnValue.fromString("helloworld" + pk + i));
            }
            batchWriteRowRequest.addRowUpdateChange(rowUpdateChange);
            // batchWriteRowRequest.addRowPutChange(rowPutChange);
        }
        final long timeSend = System.currentTimeMillis();
        final int id = start / num;
        ots.batchWriteRow(batchWriteRowRequest,
                new OTSCallback<BatchWriteRowRequest, BatchWriteRowResult>() {

                    @Override
                    public void onCompleted(
                            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext) {
                        BatchWriteRowResult result = otsContext.getOTSResult();

                        List<BatchWriteRowResult.RowStatus> statues = result
                                .getUpdateRowStatus(tableName);
                        for (BatchWriteRowResult.RowStatus status : statues) {
                            if (!status.isSucceed()) {
                                System.out.println(status.getError()
                                        .getMessage());
                            }
                        }
                        latency[id] = System.currentTimeMillis() - timeSend;
                        if (latch != null) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
                            OTSException ex) {
                        System.out.println(ex.getErrorCode());
                        ex.printStackTrace();
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
                            ClientException ex) {
                        System.out.println(ex.getCause());
                        errCount.incrementAndGet();
                        if (latch != null) {
                            latch.countDown();
                        }
                    }
                });
    }

    private static TableMeta getTestTableMeta() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER);
        return tableMeta;
    }

    private static CapacityUnit getTestTableCapacityUnit() {
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(5000);
        capacityUnit.setWriteCapacityUnit(5000);
        return capacityUnit;
    }

    private static void updateTable(OTS ots, int readCu, int writeCu) {
        UpdateTableRequest updateTableCapacityRequest = new UpdateTableRequest();
        updateTableCapacityRequest.setTableName(tableName);
        ReservedThroughputChange capacityChange = new ReservedThroughputChange();
        if (readCu != -1) {
            capacityChange.setReadCapacityUnit(readCu);
        }
        if (writeCu != -1) {
            capacityChange.setWriteCapacityUnit(writeCu);
        }
        updateTableCapacityRequest.setReservedThroughputChange(capacityChange);
        ots.updateTable(updateTableCapacityRequest);
    }

    private static void describeTable(OTS ots) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResult result = ots.describeTable(describeTableRequest);
        TableMeta tableMeta = result.getTableMeta();
        System.out.println(tableMeta.getTableName());
        for (Entry<String, PrimaryKeyType> entry : tableMeta.getPrimaryKey()
                .entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }

        System.out.println("Read:"
                + result.getReservedThroughputDetails().getCapacityUnit()
                        .getReadCapacityUnit());
        System.out.println("Write:"
                + result.getReservedThroughputDetails().getCapacityUnit()
                        .getWriteCapacityUnit());
    }

    private static void createTable() {
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(1);
        config.setIoThreadCount(1);
        OTS ots = OTSClientFactory.createOTSClient(ServiceSettings.load(),
                config);
        try {
            TableMeta tableMeta = getTestTableMeta();
            CapacityUnit tableCU = getTestTableCapacityUnit();
            CreateTableRequest createTableRequest = new CreateTableRequest();
            createTableRequest.setTableMeta(tableMeta);
            createTableRequest.setReservedThroughput(tableCU);
            ots.createTable(createTableRequest);
            Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        } catch (OTSException ex) {
            if (!ex.getErrorCode().equals("OTSObjectAlreadyExist")) {
                System.out.println(ex.getRequestId());
                ex.printStackTrace();
            }
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        } finally {
            describeTable(ots);
            ots.shutdown();
        }
    }

    private static String toHexAndReverse(int pk) {
        StringBuffer stringBuffer = new StringBuffer(Integer.toHexString(pk));
        return stringBuffer.reverse().toString();
    }

}
