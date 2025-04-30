package com.alicloud.openservices.tablestore.smoketest;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.DateUtil;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

public class SmokeTest {

    static class Test {
        private final Exception e;

        public Test(Exception e) {
            this.e = e;
        }

        public void a() throws Exception {
            e.fillInStackTrace();
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {

        Test t = new Test(new IllegalArgumentException("hello"));
        //t.a();
        SyncClient ots = new SyncClient("endpoint", "access_key_id", "access_key_secret", "instance_name");

        ClientConfiguration config = new ClientConfiguration();
        config.setRetryThreadCount(3);
        config.setRetryStrategy(new RetryStrategy() {
            @Override
            public RetryStrategy clone() {
                return this;
            }

            @Override
            public int getRetries() {
                return 0;
            }

            @Override
            public long nextPause(String action, Exception ex) {
                return 0;
            }
        });

        try {
            createTable(ots);
            try {
                listTable(ots);
                describeTable(ots);
                updateTable(ots);

                Thread.sleep(3000);
                putRow(ots);
                updateRow(ots);
                getRow(ots);
                deleteRow(ots);
                getRange(ots);
                batchWriteRow(ots);
                batchGetRow(ots);

                //streamOperations(ots);
            } finally {
                deleteTable(ots);
            }
        } finally {
            ots.shutdown();
        }

    }

    private static void batchGetRow(SyncClient ots) {
        BatchGetRowRequest request = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("HelloWorld");
        for (int i = 1; i < 20; i++) {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(i)).build();
            criteria.addRow(primaryKey);
        }
        criteria.setMaxVersions(1);
        request.addMultiRowQueryCriteria(criteria);
        BatchGetRowResponse response = ots.batchGetRow(request);

        System.out.println(response.getFailedRows().size());
        log(response.jsonize());
    }

    private static void listTable(SyncClient ots) {
        log("ListTable");
        ListTableResponse response = ots.listTable();
        log(response.jsonize());
    }

    private static void describeTable(SyncClient ots) {
        log("DescribeTable HelloWorld");
        DescribeTableRequest request = new DescribeTableRequest("HelloWorld");
        DescribeTableResponse response = ots.describeTable(request);
        log(response.jsonize());
    }

    private static void updateTable(SyncClient ots) {
        log("UpdateTable HelloWorld");
        UpdateTableRequest request = new UpdateTableRequest("HelloWorld");
        request.setTableOptionsForUpdate(
                new TableOptions(86401, 3));
        UpdateTableResponse response = ots.updateTable(request);
        log(response.jsonize());
    }

    private static void deleteRow(SyncClient ots) {
        log("DeleteRow");
        DeleteRowRequest request = new DeleteRowRequest();
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder().
                addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(100)).build();
        RowDeleteChange rowChange = new RowDeleteChange("HelloWorld", primaryKey);
        request.setRowChange(rowChange);
        DeleteRowResponse response = ots.deleteRow(request);
        log(response.jsonize());
    }

    private static void createTable(SyncClient ots) {
        log("CreateTable HelloWorld");
        TableMeta meta = new TableMeta("HelloWorld");
        meta.addPrimaryKeyColumn("PK0", PrimaryKeyType.INTEGER);
        TableOptions opts = new TableOptions();
        opts.setMaxVersions(1);
        opts.setTimeToLive(Integer.MAX_VALUE);
        CreateTableRequest ct = new CreateTableRequest(meta, opts);
        ct.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        ots.createTable(ct);
    }

    private static void deleteTable(SyncClient ots) {
        log("DeleteTable HelloWorld");
        DeleteTableRequest dt = new DeleteTableRequest("HelloWorld");
        ots.deleteTable(dt);
    }

    private static void batchWriteRow(SyncClient ots) {
        String sv = "Hangzhou";
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        for (int i = 1; i < 20; i++) {
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder().addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(i)).build();
            RowUpdateChange rowChange = new RowUpdateChange("HelloWorld", primaryKey);
            rowChange.put(new Column("Column0", ColumnValue.fromString(sv)));
            rowChange.put(new Column("Column1", ColumnValue.fromString(sv)));
            rowChange.put(new Column("Column2", ColumnValue.fromString(sv)));
            rowChange.put(new Column("Column3", ColumnValue.fromString(sv)));
            rowChange.put(new Column("Column4", ColumnValue.fromString(sv)));
            request.addRowChange(rowChange);
        }

        BatchWriteRowResponse result = ots.batchWriteRow(request);
        System.out.println(result.getFailedRows().size());
        System.out.println(result.getRequestId());
    }

    private static void getRange(SyncClient ots) {
        GetRangeRequest request = new GetRangeRequest();
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria("HelloWorld");

        PrimaryKeyColumn[] pks = new PrimaryKeyColumn[1];
        pks[0] = new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(0));
        PrimaryKey startKey = new PrimaryKey(pks);
        criteria.setInclusiveStartPrimaryKey(startKey);
        criteria.setMaxVersions(1000);

        pks = new PrimaryKeyColumn[1];
        pks[0] = new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(20));
        PrimaryKey endKey = new PrimaryKey(pks);
        criteria.setExclusiveEndPrimaryKey(endKey);
        SingleColumnValueFilter filter = new SingleColumnValueFilter("Hello",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(1));
        criteria.setFilter(filter);
        criteria.setLimit(10);
        request.setRangeRowQueryCriteria(criteria);

        GetRangeResponse result = ots.getRange(request);
        for (Row row : result.getRows()) {
            System.out.println(row);
        }

        System.out.println("NextKey: " + result.getNextStartPrimaryKey());
        System.out.println("RowsCount: " + result.getRows().size());
    }

    private static void putRow(SyncClient ots) {
        log("PutRow");
        for (int i = 0; i < 1; i++) {
            PutRowRequest request = new PutRowRequest();
            List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
            pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(i)));
            PrimaryKey primaryKey = new PrimaryKey(pks);
            RowPutChange rowChange = new RowPutChange("HelloWorld", primaryKey);
            rowChange.addColumn("Hello", ColumnValue.fromLong(0), System.currentTimeMillis());
            rowChange.addColumn("Hello", ColumnValue.fromLong(0), System.currentTimeMillis() + 1);
            rowChange.addColumn("World", ColumnValue.fromLong(0));
            request.setRowChange(rowChange);

            PutRowResponse response = ots.putRow(request);
            log("PutRow" + i + " " + response.jsonize());
        }
    }

    private static void updateRow(SyncClient ots) {
        log("UpdateRow");
        UpdateRowRequest request = new UpdateRowRequest();
        List<PrimaryKeyColumn> pks = new ArrayList<PrimaryKeyColumn>();
        pks.add(new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(3)));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        RowUpdateChange rowChange = new RowUpdateChange("HelloWorld", primaryKey);
        rowChange.put("Hello", ColumnValue.fromLong(1));
        request.setRowChange(rowChange);

        UpdateRowResponse response = ots.updateRow(request);
        log("UpdateRow " + response.jsonize());
    }

    private static void getRow(SyncClient ots) {
        GetRowRequest request = new GetRowRequest();
        PrimaryKeyColumn[] pks = new PrimaryKeyColumn[1];
        pks[0] = new PrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(3));
        PrimaryKey primaryKey = new PrimaryKey(pks);
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria("HelloWorld", primaryKey);
        //criteria.addColumnsToGet("PK0");
        criteria.setMaxVersions(10);
        /*
        SingleColumnValueCondition filter = new SingleColumnValueCondition("Hello",
                SingleColumnValueCondition.CompareOperator.EQUAL, ColumnValue.fromLong(1));
        criteria.setFilter(filter);
        */

        request.setRowQueryCriteria(criteria);
        GetRowResponse response = ots.getRow(request);
        System.out.println(response.getRow());
        log("GetRow " + response.jsonize());
    }

    private static void log(String msg) {
        System.out.println(
                String.format("%s - %s",
                        DateUtil.getCurrentIso8601Date(),
                        msg));
    }
}
