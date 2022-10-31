/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.integration;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.aliyun.openservices.ots.model.*;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSErrorCode;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.utils.ServiceSettings;

public class OTSStressTest {
    private String tableName = "ots_stress_test_table";
    private static final int MAX_RETRIES = 3;
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;
    private static final OTS ots = OTSClientFactory.createOTSClient(
            ServiceSettings.load(), new ClientConfiguration());
    private static final Logger LOG = Logger.getLogger(OTSStressTest.class.getName());

    private static class Summary {
        private int requests = 0;
        private int failures = 0;
        //        private StringBuilder message = new StringBuilder();

        public Summary() {}

        public synchronized void incrementRequests(){ requests++; }

        public synchronized void incrementFailures(){ failures++; }

        //        public synchronized void addMessage(String msg){ this.message.append(msg); }

        public int getRequests() { return requests; }

        public int getFailures() { return failures; }
    }

    private static interface OTSMethodWrapper {
        void run() throws OTSException, ClientException;
    }

    private static void retryableCallOTSMethod(OTSMethodWrapper method)
            throws OTSException, ClientException{
        int retries = 0;
        while(retries++ < MAX_RETRIES){
            try {
                method.run();
            } catch (OTSException e) {
                if (!shouldRetry(e.getErrorCode())){
                    throw e;
                }
            } catch (ClientException e) {
                throw e;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new ClientException(e);
            }
        }
    }
    
    private static boolean shouldRetry(String errorCode){
        return OTSErrorCode.ROW_OPERATION_CONFLICT.equals(errorCode);
    }

    @Before
    public void setup() throws Exception {
        LOG.info("Instance: " + ServiceSettings.load().getOTSInstanceName());

        ListTableResult r = ots.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            ots.deleteTable(deleteTableRequest);

            LOG.info("Delete table: " + table);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testConcurrentPutRow() throws Exception{
        final int CONCURRENCIES = 2;
        final int INTERVAL = 900; // in milliseconds
        final int MAX_OCCURS = 10;

        // create
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestTableCapacityUnit();
        tableMeta.addPrimaryKeyColumn("flag", PrimaryKeyType.STRING);

        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(tableMeta);
        createTableRequest.setReservedThroughput(tableCU);
        ots.createTable(createTableRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY); // Wait for the table is ready

        final Summary sum = new Summary();

        Runnable putDataRun = new Runnable(){
            public void run(){

                for(int i = 0; i < MAX_OCCURS; i++){

                    System.out.println("Loop " + i + " on the thread " + Thread.currentThread().getName());

                    final String flag = Thread.currentThread().getName() + ":" + new Date().getTime();
                    System.out.println(flag);

                    // Put data
                    try {
                        sum.incrementRequests();
                        retryableCallOTSMethod(new OTSMethodWrapper() {
                            @Override
                            public void run() throws OTSException, ClientException {
                                RowPutChange rowPutChange = new RowPutChange(tableName);
                                RowPrimaryKey pks = new RowPrimaryKey();
                                pks.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
                                pks.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("张三"));
                                pks.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromString(flag));
                                rowPutChange.setPrimaryKey(pks);
                                rowPutChange.addAttributeColumn("address", ColumnValue.fromString("北京某地"));
                                rowPutChange.addAttributeColumn("level", ColumnValue.fromLong(1));
                                rowPutChange.addAttributeColumn("dmax", ColumnValue.fromDouble(Double.MAX_VALUE));
                                rowPutChange.addAttributeColumn("dmin", ColumnValue.fromDouble(-Double.MAX_VALUE));

                                PutRowRequest putRowRequest = new PutRowRequest();
                                putRowRequest.setRowChange(rowPutChange);
                                ots.putRow(putRowRequest);
                            }
                        });
                    } catch (OTSException e) {
                        sum.incrementFailures();
                        System.err.println(flag + "[PUT] " + e.getMessage() + "\tRequestID: " + e.getRequestId() + ", HostID:" + e.getHostId());
                    } catch (ClientException e) {
                        sum.incrementFailures();
                        System.err.println("[PUT] " + e.getMessage());
                    }

                    try {
                        Thread.sleep(INTERVAL / 3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }

                    // Get data
                    SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
                    RowPrimaryKey pks = new RowPrimaryKey();
                    pks.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
                    pks.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("张三"));
                    pks.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromString(flag));
                    criteria.setPrimaryKey(pks);
                    try {
                        sum.incrementRequests();
                        GetRowRequest getRowRequest = new GetRowRequest();
                        getRowRequest.setRowQueryCriteria(criteria);
                        Row row = ots.getRow(getRowRequest).getRow();
                        assertTrue(row != null);
                    } catch (OTSException e) {
                        sum.incrementFailures();
                        System.err.println(flag + "[GET] " + e.getMessage() + "\tRequestID: " + e.getRequestId() + ", HostID:" + e.getHostId());
                    } catch (ClientException e) {
                        sum.incrementFailures();
                        System.err.println("[GET] " + e.getMessage());
                    }

                    try {
                        Thread.sleep(INTERVAL / 3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }

                    // Delete data
                    try {
                        sum.incrementRequests();
                        retryableCallOTSMethod(new OTSMethodWrapper() {

                            @Override
                            public void run() throws OTSException, ClientException {
                                RowDeleteChange rowDelChange = new RowDeleteChange(tableName);
                                RowPrimaryKey pks = new RowPrimaryKey();
                                pks.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
                                pks.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("张三"));
                                pks.addPrimaryKeyColumn("flag", PrimaryKeyValue.fromString(flag));
                                rowDelChange.setPrimaryKey(pks);

                                DeleteRowRequest deleteRowRequest = new DeleteRowRequest();
                                deleteRowRequest.setRowChange(rowDelChange);
                                ots.deleteRow(deleteRowRequest);
                            }
                        });
                    } catch (OTSException e) {
                        sum.incrementFailures();
                        System.err.println(flag + "[DELELE] " + e.getMessage() + "\tRequestID: " + e.getRequestId() + ", HostID:" + e.getHostId());
                    } catch (ClientException e) {
                        sum.incrementFailures();
                        System.err.println("[DELELE] " + e.getMessage());
                    }

                    try {
                        Thread.sleep(INTERVAL / 3);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        fail(e.getMessage());
                    }
                }
            }
        };

        // Make threads.
        Thread threads[] = new Thread[CONCURRENCIES];
        for(int i = 0; i < CONCURRENCIES; i++){
            threads[i] = new Thread(putDataRun);
            threads[i].setName("ots_thread_" + i);
        }

        // Start threads.
        for(int i = 0; i < CONCURRENCIES; i++){
            threads[i].start();
        }

        // Wait for threads to complete.
        for(int i = 0; i < CONCURRENCIES; i++){
            threads[i].join();
        }

        // Print summaries
        System.out.println("Requests in total: " + Integer.toString(sum.getRequests()));
        if (sum.getFailures() > 0){
            System.out.println("Failures: " + Integer.toString(sum.getFailures()));
            fail("Failure rate: " + Double.toString((sum.getFailures() * 100) / sum.getRequests()) + "%");
        } else {
            System.out.println("All requests succeeded.");
        }
    }

    @Test
    public void testGetRowBenchmark() throws Exception {
        final int CONCURRENCIES = 4;
        final int MAX_OCCURS = 100;
        final int ROW_COUNT = 10;

        System.out.println("Java version: " + System.getProperty("java.version"));

        ServiceSettings ss = ServiceSettings.load();
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(CONCURRENCIES);
        if (ss.getProxyHost() != null && !ss.getProxyHost().isEmpty()) {
            config.setProxyHost(ss.getProxyHost());
            config.setProxyPort(ss.getProxyPort());
        }
        final OTS ots = OTSClientFactory.createOTSClient(
                ServiceSettings.load(), config);
        // create
        TableMeta tableMeta = getTestTableMeta();
        CapacityUnit tableCU = getTestTableCapacityUnit();
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableMeta(tableMeta);
        createTableRequest.setReservedThroughput(tableCU);
        ots.createTable(createTableRequest);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY); // Wait for the table is ready

        // prepare data
        for(int i = 0; i < ROW_COUNT; i++) {
            RowPutChange rowPutChange = new RowPutChange(tableName);
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
            pks.addPrimaryKeyColumn("name", PrimaryKeyValue.fromString("张三" + String.valueOf(i)));
            rowPutChange.setPrimaryKey(pks);
            rowPutChange.addAttributeColumn("address", ColumnValue.fromString("北京某地"));
            rowPutChange.addAttributeColumn("level", ColumnValue.fromLong(1));

            PutRowRequest putRowRequest = new PutRowRequest();
            putRowRequest.setRowChange(rowPutChange);
            ots.putRow(putRowRequest);
        }

        // get data
        Runnable getDataFunc = new Runnable() {

            @Override
            public void run() {
                final String threadName = Thread.currentThread().getName();
                System.out.println("The thread '" + threadName + "'started.");
                long startTimeMillis = System.currentTimeMillis();

                for(int i = 0; i < MAX_OCCURS; i++) {
                    RangeIteratorParameter criteria = new RangeIteratorParameter(tableName);
                    RowPrimaryKey inclusiveStartPrimaryKey = new RowPrimaryKey();
                    RowPrimaryKey exclusiveEndPrimaryKey = new RowPrimaryKey();
                    criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
                    criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
                    inclusiveStartPrimaryKey.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
                    inclusiveStartPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MIN);

                    exclusiveEndPrimaryKey.addPrimaryKeyColumn("groupid", PrimaryKeyValue.fromLong(1));
                    exclusiveEndPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MAX);

                    try {
                        List<Row> rows = new ArrayList<Row>();
                        Iterator<Row> iter = ots.createRangeIterator(criteria);
                        while (iter.hasNext()) {
                            rows.add(iter.next());
                        }
                        assertEquals(ROW_COUNT, rows.size());
                    } catch (OTSException e) {
                        System.err.println(threadName + "[GET] OTSException: " + e.getMessage() +
                                "\tRequestID: " + e.getRequestId() + ", HostID:" + e.getHostId());
                    } catch (ClientException e) {
                        System.err.println(threadName + "[GET] ClientException: " + e.getMessage());
                    } catch (Exception e) {
                        System.err.println(threadName + "[GET] Exception: " + e.getMessage());
                    }
                }
                long endTimeMillis = System.currentTimeMillis();
                long averageMillis = (endTimeMillis - startTimeMillis) / MAX_OCCURS;
                System.out.println("Average time on " + threadName + ": "
                        + String.valueOf(averageMillis) + "ms");
            }
        };

        // Make threads.
        ExecutorService executor = Executors.newCachedThreadPool();
        for(int i = 0; i < CONCURRENCIES; i++) {
            executor.submit(getDataFunc);
        }
        executor.shutdown();
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
        }
    }

    private TableMeta getTestTableMeta() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("groupid", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("name", PrimaryKeyType.STRING);
        return tableMeta;
    }
    
    private CapacityUnit getTestTableCapacityUnit() {
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(5000);
        capacityUnit.setWriteCapacityUnit(5000);
        return capacityUnit;
    }
}
