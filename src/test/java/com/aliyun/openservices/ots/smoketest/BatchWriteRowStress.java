package com.aliyun.openservices.ots.smoketest;

import com.aliyun.openservices.ots.*;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;
import com.aliyun.openservices.ots.model.*;

import java.util.ArrayList;
import java.util.List;

public class BatchWriteRowStress {

    private static final String TABLE_NAME = "HelloWorld";

    public static class BatchWriteRowThread implements Runnable {
        private OTS ots;

        public BatchWriteRowThread(OTS ots) {
            this.ots = ots;
        }


        @Override
        public void run() {
            while (true) {
            //for (int d = 0; d < 2; d++) {
                try {
                    BatchWriteRowRequest request = new BatchWriteRowRequest();
                    for (int i = 0; i < 120; i++) {
                        RowPutChange rowChange = new RowPutChange(TABLE_NAME);
                        RowPrimaryKey primaryKey = new RowPrimaryKey();
                        primaryKey.addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(10));
                        primaryKey.addPrimaryKeyColumn("PK2", PrimaryKeyValue.fromLong(99));
                        primaryKey.addPrimaryKeyColumn("PK3", PrimaryKeyValue.fromLong(99));
                        rowChange.setPrimaryKey(primaryKey);
                        for (int j = 11; j < 25; j++) {
                            rowChange.addAttributeColumn("Column" + j, ColumnValue.fromLong(j));
                        }
                        request.addRowPutChange(rowChange);
                    }
                    ots.batchWriteRow(request);
                } catch (OTSException e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    public static void createTable(OTS ots) {
        CreateTableRequest request = new CreateTableRequest();
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(new CapacityUnit(1000000, 10000000));
        ots.createTable(request);
    }

    public static void main(String[] args) throws Exception {
        ClientConfiguration cc = new ClientConfiguration();
        OTSServiceConfiguration sc = new OTSServiceConfiguration();
        sc.setEnableResponseValidation(false);
        cc.setIoThreadCount(1);
        cc.setMaxConnections(1);
        OTSRetryStrategy retryStrategy = new OTSRetryStrategy() {

            @Override
            public boolean shouldRetry(String action, Exception ex, int retries) {
                return false;
            }

            @Override
            public long getPauseDelay(String action, Exception ex, int retries) {
                return 10;
            }

        };
        sc.setRetryStrategy(retryStrategy);

        OTS ots = null;

        if (false) {
            ots = new OTSClient("http://10.101.166.28:8888",
                    "accessid_for_zzf_test", "accesskey_for_zzf_testt", "hellotest",
                    cc, sc);
        } else {
            ots = new OTSClient("http://10.101.168.147:9090",
                    "accessid_for_zzf_test", "accesskey_for_zzf_testt", "hellotest",
                    cc, sc);
        }

        //createTable(ots);

        try {

            List<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < 1; i++) {
                threads.add(new Thread(new BatchWriteRowThread(ots)));
            }

            for (Thread t : threads) {
                t.start();
            }

            for (Thread t : threads) {
                t.join();
            }

        }finally {
            ots.shutdown();
        }
    }
}
