/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.smoketest;

import java.util.List;
import java.util.Map.Entry;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSClientAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.CreateTableResult;
import com.aliyun.openservices.ots.model.DeleteRowResult;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableResult;
import com.aliyun.openservices.ots.model.ReservedThroughputChange;
import com.aliyun.openservices.ots.model.CapacityUnit;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.DeleteRowRequest;
import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.GetRowRequest;
import com.aliyun.openservices.ots.model.GetRowResult;
import com.aliyun.openservices.ots.model.ListTableRequest;
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.MultiRowQueryCriteria;
import com.aliyun.openservices.ots.model.OTSContext;
import com.aliyun.openservices.ots.model.OTSFuture;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.PutRowResult;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowDeleteChange;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;
import com.aliyun.openservices.ots.model.SingleRowQueryCriteria;
import com.aliyun.openservices.ots.model.TableMeta;
import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowResult;
import com.aliyun.openservices.ots.model.UpdateTableRequest;
import com.aliyun.openservices.ots.model.UpdateTableResult;
import com.aliyun.openservices.ots.utils.ServiceSettings;

public class SmokeTestForAsync {
    private static OTSAsync ots;
    private static String endpoint;
    private static String accessId;
    private static String accessKey;
    private static String instanceName;
    private static CountDownLatch latch;
    private static AtomicLong count;

    public static void main(String[] args) throws Exception {
        ServiceSettings ss = ServiceSettings.load();
        endpoint = ss.getOTSEndpoint();
        accessId = ss.getOTSAccessKeyId();
        accessKey = ss.getOTSAccessKeySecret();
        instanceName = ss.getOTSInstanceName();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setEnableRequestCompression(false);
        osc.setEnableResponseCompression(false);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(30);
        ots = new OTSClientAsync(endpoint, accessId, accessKey, instanceName,
                config, osc, pool);
        count = new AtomicLong();
         createTable("sdk_test");
         listTable();
        // createTable("test_why");
//        updateTable("test_why", 50000, 50000);
//        latch = new CountDownLatch(100);
//        describeTable("test_why");
//        long time0 = System.currentTimeMillis();
//        for (int i = 0; i < 100; i++) {
//            putRow("test_why", "" + i);
//        }
//        latch.await();
//        System.out.println(System.currentTimeMillis() - time0);
//        System.out.println("done");
//        ots.shutdown();
    }

    private static OTSFuture<ListTableResult> listTable() {
        OTSFuture<ListTableResult> future = ots
                .listTable(new OTSCallback<ListTableRequest, ListTableResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<ListTableRequest, ListTableResult> otsContext) {
                        ListTableResult result = otsContext.getOTSResult();
                        for (String tableName : result.getTableNames()) {
                            System.out.println(tableName);
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<ListTableRequest, ListTableResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub
                        ex.printStackTrace();
                    }

                    @Override
                    public void onFailed(
                            OTSContext<ListTableRequest, ListTableResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub
                        ex.printStackTrace();
                    }
                });
        return future;
    }

    private static void listTableWithoutCallback() {
        try {
            OTSFuture<ListTableResult> future = ots.listTable();
            ListTableResult result = future.get();
            for (String tableName : result.getTableNames()) {
                System.out.println(tableName);
            }
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void createTable(String tableName) {
        CreateTableRequest request = new CreateTableRequest();
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(5000);
        capacityUnit.setWriteCapacityUnit(5000);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        OTSFuture<CreateTableResult> future = ots.createTable(request,
                new OTSCallback<CreateTableRequest, CreateTableResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<CreateTableRequest, CreateTableResult> otsContext) {
                        OTSFuture<ListTableResult> future = ots.listTable();
                        ListTableResult result = future.get();
                        for (String tableName : result.getTableNames()) {
                            System.out.println(tableName);
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<CreateTableRequest, CreateTableResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub
                        System.out.println("failed");
                    }

                    @Override
                    public void onFailed(
                            OTSContext<CreateTableRequest, CreateTableResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
        try {
            future.get();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void createTableWithoutCallback(String tableName) {
        CreateTableRequest request = new CreateTableRequest();
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(1000);
        capacityUnit.setWriteCapacityUnit(1000);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        try {
            OTSFuture<CreateTableResult> future = ots.createTable(request);
            CreateTableResult result = future.get();
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void describeTable(String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        OTSFuture<DescribeTableResult> future = ots.describeTable(
                describeTableRequest,
                new OTSCallback<DescribeTableRequest, DescribeTableResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<DescribeTableRequest, DescribeTableResult> otsContext) {
                        DescribeTableResult result = otsContext.getOTSResult();
                        TableMeta tableMeta = result.getTableMeta();
                        System.out.println(tableMeta.getTableName());
                        for (Entry<String, PrimaryKeyType> entry : tableMeta
                                .getPrimaryKey().entrySet()) {
                            System.out.println(entry.getKey() + ":"
                                    + entry.getValue());
                        }
                        System.out.println("Read:"
                                + result.getReservedThroughputDetails()
                                        .getCapacityUnit()
                                        .getReadCapacityUnit());
                        System.out.println("Write:"
                                + result.getReservedThroughputDetails()
                                        .getCapacityUnit()
                                        .getWriteCapacityUnit());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<DescribeTableRequest, DescribeTableResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<DescribeTableRequest, DescribeTableResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void describeTableWithoutCallback(String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        try {
            OTSFuture<DescribeTableResult> future = ots
                    .describeTable(describeTableRequest);
            DescribeTableResult result = future.get();
            TableMeta tableMeta = result.getTableMeta();
            System.out.println(tableMeta.getTableName());
            for (Entry<String, PrimaryKeyType> entry : tableMeta
                    .getPrimaryKey().entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
            System.out.println("Read:"
                    + result.getReservedThroughputDetails().getCapacityUnit()
                            .getReadCapacityUnit());
            System.out.println("Write:"
                    + result.getReservedThroughputDetails().getCapacityUnit()
                            .getWriteCapacityUnit());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void updateTable(String tableName, int readCu, int writeCu) {
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
        OTSFuture<UpdateTableResult> future = ots.updateTable(
                updateTableCapacityRequest,
                new OTSCallback<UpdateTableRequest, UpdateTableResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<UpdateTableRequest, UpdateTableResult> otsContext) {
                        // TODO Auto-generated method stub
                        UpdateTableResult result = otsContext.getOTSResult();
                        System.out.println("LastDecreaseTime: "
                                + result.getReservedThroughputDetails()
                                        .getLastDecreaseTime());
                        System.out.println("LastIncreaseTime: "
                                + result.getReservedThroughputDetails()
                                        .getLastIncreaseTime());
                        System.out.println("DecreaseCount: "
                                + result.getReservedThroughputDetails()
                                        .getNumberOfDecreasesToday());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<UpdateTableRequest, UpdateTableResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub
                        ex.printStackTrace();
                    }

                    @Override
                    public void onFailed(
                            OTSContext<UpdateTableRequest, UpdateTableResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub
                        ex.printStackTrace();

                    }
                });
    }

    private static void updateTableWithoutCallback(String tableName,
            int readCu, int writeCu) {
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
        try {
            OTSFuture<UpdateTableResult> future = ots
                    .updateTable(updateTableCapacityRequest);
            UpdateTableResult result = future.get();
            System.out.println("LastDecreaseTime: "
                    + result.getReservedThroughputDetails()
                            .getLastDecreaseTime());
            System.out.println("LastIncreaseTime: "
                    + result.getReservedThroughputDetails()
                            .getLastIncreaseTime());
            System.out.println("DecreaseCount: "
                    + result.getReservedThroughputDetails()
                            .getNumberOfDecreasesToday());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void deleteTable(String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        OTSFuture<DeleteTableResult> future = ots.deleteTable(
                deleteTableRequest,
                new OTSCallback<DeleteTableRequest, DeleteTableResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<DeleteTableRequest, DeleteTableResult> otsContext) {
                        System.out.println(otsContext.getOTSRequest()
                                .getTableName());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<DeleteTableRequest, DeleteTableResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<DeleteTableRequest, DeleteTableResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void deleteTableWithoutCallback(String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        try {
            OTSFuture<DeleteTableResult> future = ots
                    .deleteTable(deleteTableRequest);
            DeleteTableResult result = future.get();
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void getRow(String tableName, String pk) {
        GetRowRequest getRowRequest = new GetRowRequest();
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowQueryCriteria.setPrimaryKey(pks);
        getRowRequest.setRowQueryCriteria(rowQueryCriteria);

        OTSFuture<GetRowResult> future = ots.getRow(getRowRequest,
                new OTSCallback<GetRowRequest, GetRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<GetRowRequest, GetRowResult> otsContext) {
                        GetRowResult result = otsContext.getOTSResult();
                        // Row row = result.getRow();
                        // for (Entry<String, ColumnValue> entry : row
                        // .getColumns().entrySet()) {
                        // System.out.println(entry.getKey() + ":"
                        // + entry.getValue());
                        // }
                        // System.out.println("ReadCapacity:"
                        // + result.getConsumedCapacity()
                        // .getCapacityUnit()
                        // .getReadCapacityUnit());
                        // System.out.println("WriteCapacity:"
                        // + result.getConsumedCapacity()
                        // .getCapacityUnit()
                        // .getWriteCapacityUnit());
                        latch.countDown();
                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRowRequest, GetRowResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub
                        System.out.println(ex.getErrorCode());
                        latch.countDown();
                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRowRequest, GetRowResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub
                        System.out.println(ex.getErrorCode());
                        latch.countDown();
                    }
                });
    }

    private static void getRowWithoutCallback(String tableName, String pk) {
        GetRowRequest getRowRequest = new GetRowRequest();
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowQueryCriteria.setPrimaryKey(pks);
        getRowRequest.setRowQueryCriteria(rowQueryCriteria);
        try {
            OTSFuture<GetRowResult> future = ots.getRow(getRowRequest);
            GetRowResult result = future.get();
            Row row = result.getRow();
            for (Entry<String, ColumnValue> entry : row.getColumns().entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
            System.out.println("ReadCapacity:"
                    + result.getConsumedCapacity().getCapacityUnit()
                            .getReadCapacityUnit());
            System.out.println("WriteCapacity:"
                    + result.getConsumedCapacity().getCapacityUnit()
                            .getWriteCapacityUnit());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void putRow(String tableName, String pk) {
        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        request.setRowChange(rowChange);
        final long timeSt = new Date().getTime();
        OTSFuture<PutRowResult> future = ots.putRow(request,
                new OTSCallback<PutRowRequest, PutRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<PutRowRequest, PutRowResult> otsContext) {
                        PutRowResult result = otsContext.getOTSResult();
                        latch.countDown();
                        // System.out.println(System.currentTimeMillis());
                        // System.out.println(result.getConsumedCapacity()
                        // .getCapacityUnit().getReadCapacityUnit());
                        // System.out.println(result.getConsumedCapacity()
                        // .getCapacityUnit().getWriteCapacityUnit());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<PutRowRequest, PutRowResult> otsContext,
                            OTSException ex) {
                        latch.countDown();
                        System.out.println(ex.getErrorCode());
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<PutRowRequest, PutRowResult> otsContext,
                            ClientException ex) {
                        latch.countDown();
                        System.out.println(ex.getErrorCode());
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void putRowWithoutCallback(String tableName, String pk) {
        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        request.setRowChange(rowChange);
        try {
            OTSFuture<PutRowResult> future = ots.putRow(request);
            PutRowResult result = future.get();
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void updateRow(String tableName, String pk) {
        UpdateRowRequest updateRowRequest = new UpdateRowRequest();
        RowUpdateChange rowChange = new RowUpdateChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        rowChange.addAttributeColumn("col0", ColumnValue.fromBoolean(true));
        updateRowRequest.setRowChange(rowChange);
        OTSFuture<UpdateRowResult> future = ots.updateRow(updateRowRequest,
                new OTSCallback<UpdateRowRequest, UpdateRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<UpdateRowRequest, UpdateRowResult> otsContext) {
                        UpdateRowResult result = otsContext.getOTSResult();
                        System.out.println(result.getConsumedCapacity()
                                .getCapacityUnit().getReadCapacityUnit());
                        System.out.println(result.getConsumedCapacity()
                                .getCapacityUnit().getWriteCapacityUnit());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<UpdateRowRequest, UpdateRowResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<UpdateRowRequest, UpdateRowResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void updateRowWithoutCallback(String tableName, String pk) {
        UpdateRowRequest updateRowRequest = new UpdateRowRequest();
        RowUpdateChange rowChange = new RowUpdateChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        rowChange.addAttributeColumn("col0", ColumnValue.fromBoolean(true));
        updateRowRequest.setRowChange(rowChange);
        try {
            OTSFuture<UpdateRowResult> future = ots.updateRow(updateRowRequest);
            UpdateRowResult result = future.get();
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void deleteRow(String tableName, String pk) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest();
        RowDeleteChange rowChange = new RowDeleteChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        deleteRowRequest.setRowChange(rowChange);
        OTSFuture<DeleteRowResult> future = ots.deleteRow(deleteRowRequest,
                new OTSCallback<DeleteRowRequest, DeleteRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<DeleteRowRequest, DeleteRowResult> otsContext) {
                        DeleteRowResult result = otsContext.getOTSResult();
                        System.out.println(result.getConsumedCapacity()
                                .getCapacityUnit().getReadCapacityUnit());
                        System.out.println(result.getConsumedCapacity()
                                .getCapacityUnit().getWriteCapacityUnit());
                    }

                    @Override
                    public void onFailed(
                            OTSContext<DeleteRowRequest, DeleteRowResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<DeleteRowRequest, DeleteRowResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void deleteRowWithoutCallback(String tableName, String pk) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest();
        RowDeleteChange rowChange = new RowDeleteChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        deleteRowRequest.setRowChange(rowChange);
        try {
            OTSFuture<DeleteRowResult> future = ots.deleteRow(deleteRowRequest);
            DeleteRowResult result = future.get();
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
        } catch (ClientException ex) {
            ex.printStackTrace();
        } catch (OTSException ex) {
            System.out.println(ex.getRequestId());
            System.out.println(ex.getErrorCode());
        }
    }

    private static void batchGetRow(String tableName) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        {
            MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(
                    tableName);
            criteria.addColumnsToGet("col");
            RowPrimaryKey pk1 = new RowPrimaryKey();
            pk1.addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromString("a"));
            pk1.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("a"));
            criteria.addRow(pk1);
            batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        }
        OTSFuture<BatchGetRowResult> future = ots.batchGetRow(
                batchGetRowRequest,
                new OTSCallback<BatchGetRowRequest, BatchGetRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext) {
                        BatchGetRowResult result = otsContext.getOTSResult();
                        List<RowStatus> statues = result
                                .getBatchGetRowStatus("zzf");
                        for (RowStatus status : statues) {
                            System.out.println(status.isSucceed());
                            System.out.println(status.getError().getMessage());
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchGetRowRequest, BatchGetRowResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void batchGetRowWithoutCallback() {

    }

    private static void batchWriteRow(final String tableName) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1100; i++) {
            sb.append('c');
        }
        String longC = sb.toString();
        for (int i = 0; i < 100; i++) {
            RowPutChange rowPutChange = new RowPutChange(tableName);
            RowPrimaryKey pks = new RowPrimaryKey();
            pks.addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromString("ee" + i));
            pks.addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("ee"));
            rowPutChange.setPrimaryKey(pks);
            rowPutChange.addAttributeColumn("COL1", ColumnValue.fromLong(999));
            for (int j = 0; j < 10; j++) {
                rowPutChange.addAttributeColumn("COL2" + j,
                        ColumnValue.fromString(longC));
            }
            batchWriteRowRequest.addRowPutChange(rowPutChange);
        }

        OTSFuture<BatchWriteRowResult> future = ots.batchWriteRow(
                batchWriteRowRequest,
                new OTSCallback<BatchWriteRowRequest, BatchWriteRowResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<BatchWriteRowRequest, com.aliyun.openservices.ots.model.BatchWriteRowResult> otsContext) {
                        BatchWriteRowResult result = otsContext.getOTSResult();
                        List<BatchWriteRowResult.RowStatus> statues = result
                                .getPutRowStatus(tableName);
                        for (BatchWriteRowResult.RowStatus status : statues) {
                            System.out.println(status.isSucceed());
                            if (!status.isSucceed()) {
                                System.out.println(status.getError()
                                        .getMessage());
                            }
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<BatchWriteRowRequest, BatchWriteRowResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
    }

    private static void batchWriteRowWithoutCallback() {

    }

    private static void getRange(String tableName) {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(
                tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        RowPrimaryKey end = new RowPrimaryKey();

        start.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("uid", PrimaryKeyValue.INF_MAX);

        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(start);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);

        rangeRowQueryCriteria.setLimit(10);
        rangeRowQueryCriteria.addColumnsToGet(new String[] { "COL1", "uid" });

        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        OTSFuture<GetRangeResult> future = ots.getRange(getRangeRequest,
                new OTSCallback<GetRangeRequest, GetRangeResult>() {
                    @Override
                    public void onCompleted(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext) {
                        GetRangeResult result = otsContext.getOTSResult();
                        System.out.println("NextStartKey:"
                                + result.getNextStartPrimaryKey());
                        for (Row row : result.getRows()) {
                            for (Entry<String, ColumnValue> entry : row
                                    .getColumns().entrySet()) {
                                System.out.println(entry.getKey() + ":"
                                        + entry.getValue());
                            }
                        }
                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext,
                            OTSException ex) {
                        // TODO Auto-generated method stub

                    }

                    @Override
                    public void onFailed(
                            OTSContext<GetRangeRequest, GetRangeResult> otsContext,
                            ClientException ex) {
                        // TODO Auto-generated method stub

                    }
                });
        GetRangeResult result = future.get();
    }

}
