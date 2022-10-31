package com.aliyun.openservices.ots.smoketest;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.integration.OTSClientFactory;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class Test {


    private static OTS ots = OTSClientFactory.createOTSClient();
    private static OTSAsync otsAsync = OTSClientFactory.createOTSClientAsync();
    private static String tableName = "iteratorTest";

    private void createTable() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.INTEGER);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta);
        createTableRequest.setReservedThroughput(new CapacityUnit(5000, 5000));
        ots.createTable(createTableRequest);
    }

    private void prepareData(int n) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(n);
        final Semaphore semaphore = new Semaphore(300);
        for (int i = 0; i < n; i++) {
            RowPrimaryKey rowPrimaryKey = new RowPrimaryKey();
            rowPrimaryKey.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(i));
            RowPutChange rowPutChange = new RowPutChange(tableName);
            rowPutChange.setPrimaryKey(rowPrimaryKey);
            rowPutChange.addAttributeColumn("col" + i, ColumnValue.fromLong(i));
            if ((i % 1000) == 0) {
                System.out.println(i);
            }
            semaphore.acquire();
            otsAsync.putRow(new PutRowRequest(rowPutChange), new OTSCallback<PutRowRequest, PutRowResult>() {
                @Override
                public void onCompleted(OTSContext<PutRowRequest, PutRowResult> otsContext) {
                    semaphore.release();
                    latch.countDown();
                }

                @Override
                public void onFailed(OTSContext<PutRowRequest, PutRowResult> otsContext, OTSException ex) {
                    semaphore.release();
                    latch.countDown();
                    ex.printStackTrace();
                }

                @Override
                public void onFailed(OTSContext<PutRowRequest, PutRowResult> otsContext, ClientException ex) {
                    semaphore.release();
                    latch.countDown();
                    ex.printStackTrace();
                }
            });
        }
        latch.await();
    }

    private void rangeIterate(String colName) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(tableName);
        RowPrimaryKey rowPrimaryKey = new RowPrimaryKey();
        rowPrimaryKey.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
        rangeIteratorParameter.setInclusiveStartPrimaryKey(rowPrimaryKey);
        rowPrimaryKey = new RowPrimaryKey();
        rowPrimaryKey.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);
        rangeIteratorParameter.setExclusiveEndPrimaryKey(rowPrimaryKey);

        rangeIteratorParameter.setColumnsToGet(Arrays.asList(colName));
        Iterator<Row> iterator = ots.createRangeIterator(rangeIteratorParameter);
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    private void testGetRange() throws InterruptedException {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        criteria.setInclusiveStartPrimaryKey(new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN));
        criteria.setExclusiveEndPrimaryKey(new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX));
        criteria.setColumnsToGet(Arrays.asList("col"));
        GetRangeRequest request = new GetRangeRequest(criteria);

        final Semaphore semaphore = new Semaphore(20);
        for (int i = 0; i < 100000; i++) {
            semaphore.acquire();
            otsAsync.getRange(request, new OTSCallback<GetRangeRequest, GetRangeResult>() {
                @Override
                public void onCompleted(OTSContext<GetRangeRequest, GetRangeResult> otsContext) {
                    semaphore.release();
                    System.out.println(otsContext.getOTSResult().getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
                }

                @Override
                public void onFailed(OTSContext<GetRangeRequest, GetRangeResult> otsContext, OTSException ex) {
                    semaphore.release();
                    ex.printStackTrace();
                }

                @Override
                public void onFailed(OTSContext<GetRangeRequest, GetRangeResult> otsContext, ClientException ex) {
                    semaphore.release();
                    ex.printStackTrace();
                }
            });
        }

        /*
        GetRangeResult result = ots.getRange(request);
        System.out.println(result.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
        System.out.println(result.getRows());
        System.out.println(result.getNextStartPrimaryKey());
        */

        SingleRowQueryCriteria singleRowQueryCriteria = new SingleRowQueryCriteria(tableName);
        singleRowQueryCriteria.setPrimaryKey(new RowPrimaryKey().addPrimaryKeyColumn("pk", PrimaryKeyValue.fromLong(100000)));
        GetRowRequest getRowRequest = new GetRowRequest(singleRowQueryCriteria);
        GetRowResult getRowResult = ots.getRow(getRowRequest);
        System.out.println(getRowResult.getRow());
    }

    private void test() throws InterruptedException {
        createTable();
        prepareData(100001);
        testGetRange();
        ListTableResult listTableResult = ots.listTable();
        System.out.println(listTableResult.getTableNames());
    }

    public static void main(String[] args) throws InterruptedException {
        Test test = new Test();
        UpdateTableRequest updateTableRequest = new UpdateTableRequest(tableName);
        ReservedThroughputChange reservedThroughputChange = new ReservedThroughputChange();
        reservedThroughputChange.setReadCapacityUnit(100);
        reservedThroughputChange.setWriteCapacityUnit(100);
        updateTableRequest.setReservedThroughputChange(reservedThroughputChange);
   //     ots.updateTable(updateTableRequest);
        test.test();
        ots.shutdown();
        otsAsync.shutdown();
    }

}
