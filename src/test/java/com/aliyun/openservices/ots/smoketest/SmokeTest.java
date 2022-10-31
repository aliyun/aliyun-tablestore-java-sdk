/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.smoketest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.ServiceException;
import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.BatchGetRowRequest;
import com.aliyun.openservices.ots.model.BatchGetRowResult;
import com.aliyun.openservices.ots.model.BatchGetRowResult.RowStatus;
import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.BatchWriteRowResult;
import com.aliyun.openservices.ots.model.DeleteRowResult;
import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableResult;
import com.aliyun.openservices.ots.model.RangeIteratorParameter;
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
import com.aliyun.openservices.ots.model.ListTableResult;
import com.aliyun.openservices.ots.model.MultiRowQueryCriteria;
import com.aliyun.openservices.ots.model.OTSContext;
import com.aliyun.openservices.ots.model.OTSFuture;
import com.aliyun.openservices.ots.model.OTSResult;
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

public class SmokeTest {
    private static OTS ots;
    private static String endpoint;
    private static String accessId;
    private static String accessKey;
    private static String instanceName;

    public static void main(String[] args) throws Exception {
        ServiceSettings ss = ServiceSettings.load();
        endpoint = ss.getOTSEndpoint();
        accessId = ss.getOTSAccessKeyId();
        accessKey = ss.getOTSAccessKeySecret();
        instanceName = ss.getOTSInstanceName();
        OTSServiceConfiguration osc = new OTSServiceConfiguration();
        osc.setEnableRequestCompression(false);
        osc.setEnableResponseCompression(false);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        ClientConfiguration config = new ClientConfiguration();
        ots = new OTSClient(endpoint, accessId, accessKey, instanceName,
                config, osc);
        String tableName = "sdk_test";
//         createTable(tableName);
//         putRow(tableName, "aaa");
//         deleteTable(tableName);
//        listTable();
//        describeTable(tableName);
        // updateTable(tableName, 5000, 5000);
        // deleteTable(tableName);
//        updateRow(tableName, "aaa");
       
//        putRow(tableName, "bbb");
//        putRow(tableName, "ccc");
//        getRow(tableName, "aaa");
//        deleteRow(tableName, "aaa");
//        batchGetRow(tableName);
//        batchWriteRow(tableName);
        getRange(tableName);
//        getRangeByIterator(tableName);
        ots.shutdown();
    }

    private static void listTable() {
        ListTableResult result = ots.listTable();
        for (String tableName : result.getTableNames()) {
            System.out.println(tableName);
        }
    }

    private static void createTable(String tableName) {
        CreateTableRequest request = new CreateTableRequest();
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk", PrimaryKeyType.STRING);
        CapacityUnit capacityUnit = new CapacityUnit();
        capacityUnit.setReadCapacityUnit(1000);
        capacityUnit.setWriteCapacityUnit(1000);
        request.setTableMeta(tableMeta);
        request.setReservedThroughput(capacityUnit);
        try {
            ots.createTable(request);
        } catch (OTSException ex) {
            System.out.println(ex.getErrorCode());
            ex.printStackTrace();
        }
    }

    private static void describeTable(String tableName) {
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
        UpdateTableResult result = ots.updateTable(updateTableCapacityRequest);

        System.out.println("LastDecreaseTime: "
                + result.getReservedThroughputDetails().getLastDecreaseTime());
        System.out.println("LastIncreaseTime: "
                + result.getReservedThroughputDetails().getLastIncreaseTime());
        System.out.println("DecreaseCount: "
                + result.getReservedThroughputDetails()
                        .getNumberOfDecreasesToday());
    }

    private static void deleteTable(String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
        deleteTableRequest.setTableName(tableName);
        ots.deleteTable(deleteTableRequest);
    }

    private static void getRow(String tableName, String pk) {
        GetRowRequest getRowRequest = new GetRowRequest();
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowQueryCriteria.setPrimaryKey(pks);
        getRowRequest.setRowQueryCriteria(rowQueryCriteria);
        GetRowResult result = ots.getRow(getRowRequest);
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
    }

    private static void putRow(String tableName, String pk) {
        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        request.setRowChange(rowChange);
        long timeSt = new Date().getTime();
        try {
            PutRowResult result = ots.putRow(request);
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(result.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
            System.out.println(new Date().getTime() - timeSt);
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
        UpdateRowResult result = ots.updateRow(updateRowRequest);
        System.out.println(result.getConsumedCapacity().getCapacityUnit()
                .getReadCapacityUnit());
        System.out.println(result.getConsumedCapacity().getCapacityUnit()
                .getWriteCapacityUnit());
    }

    private static void deleteRow(String tableName, String pk) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest();
        RowDeleteChange rowChange = new RowDeleteChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(pk));
        rowChange.setPrimaryKey(pks);
        deleteRowRequest.setRowChange(rowChange);
        DeleteRowResult result = ots.deleteRow(deleteRowRequest);
        System.out.println(result.getConsumedCapacity().getCapacityUnit()
                .getReadCapacityUnit());
        System.out.println(result.getConsumedCapacity().getCapacityUnit()
                .getWriteCapacityUnit());
    }

    private static void batchGetRow(String tableName) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addColumnsToGet("col");
        RowPrimaryKey pk1 = new RowPrimaryKey();
        pk1.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("a"));
        criteria.addRow(pk1);
        batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        BatchGetRowResult result = ots.batchGetRow(batchGetRowRequest);
        List<RowStatus> statues = result.getBatchGetRowStatus(tableName);
        for (RowStatus status : statues) {
            System.out.println(status.isSucceed());
            System.out.println(status.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(status.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
            if (!status.isSucceed()) {
                System.out.println(status.getError().getMessage());
            }
        }
    }

    private static void batchWriteRow(String tableName) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        RowPutChange rowPutChange = new RowPutChange(tableName);
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString("ee"));
        rowPutChange.setPrimaryKey(pks);
        rowPutChange.addAttributeColumn("COL1", ColumnValue.fromLong(999));
        for (int j = 0; j < 10; j++) {
            rowPutChange.addAttributeColumn("COL2" + j,
                    ColumnValue.fromString("" + j));
        }
        batchWriteRowRequest.addRowPutChange(rowPutChange);
        BatchWriteRowResult result = ots.batchWriteRow(batchWriteRowRequest);
        List<BatchWriteRowResult.RowStatus> statues = result
                .getPutRowStatus(tableName);
        for (BatchWriteRowResult.RowStatus status : statues) {
            System.out.println(status.isSucceed());
            System.out.println(status.getConsumedCapacity().getCapacityUnit()
                    .getReadCapacityUnit());
            System.out.println(status.getConsumedCapacity().getCapacityUnit()
                    .getWriteCapacityUnit());
            if (!status.isSucceed()) {
                System.out.println(status.getError().getMessage());
            }
        }
    }

    private static void getRange(String tableName) {
        GetRangeRequest getRangeRequest = new GetRangeRequest();
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(
                tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        RowPrimaryKey end = new RowPrimaryKey();

        start.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);

        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(start);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(end);

        rangeRowQueryCriteria.setLimit(-1);
        rangeRowQueryCriteria.addColumnsToGet(new String[] {  "pk" });

        getRangeRequest.setRangeRowQueryCriteria(rangeRowQueryCriteria);
        GetRangeResult result = ots.getRange(getRangeRequest);
        System.out.println("NextStartKey:" + result.getNextStartPrimaryKey());

        for (Row row : result.getRows()) {
            for (Entry<String, ColumnValue> entry : row.getColumns().entrySet()) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
    }

    private static void getRangeByIterator(String tableName) {
        RangeIteratorParameter parameter = new RangeIteratorParameter(tableName);
        RowPrimaryKey start = new RowPrimaryKey();
        RowPrimaryKey end = new RowPrimaryKey();

        start.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MIN);
        end.addPrimaryKeyColumn("pk", PrimaryKeyValue.INF_MAX);

        parameter.setInclusiveStartPrimaryKey(start);
        parameter.setExclusiveEndPrimaryKey(end);

        Iterator<Row> rows = ots.createRangeIterator(parameter);
        List<Row> result = new ArrayList<Row>();
        while (rows.hasNext()) {
            result.add(rows.next());
        }

        System.out.println(result.size());
    }
}
