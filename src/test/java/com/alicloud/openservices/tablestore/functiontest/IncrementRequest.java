package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IncrementRequest {
    private static final String tableName = "IncrementRequestTest002";
    private static final String tableNameWithAutoPK = "IncrementRequestTestWithAutoPK";
    private static final String pk = "pk";
    private static final String autoPk1 = "pk1";
    private static final String autoPk2 = "pk2";
    private AsyncClientInterface ots;
    private static ServiceSettings serviceSettings = ServiceSettings.load();

    public void createTable(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(pk, PrimaryKeyType.STRING);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setTableOptions(tableOptions);

        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }

    public void createTableWithAutoPK(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(tableNameWithAutoPK);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(autoPk1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(autoPk2, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

        int maxVersions = 1; // 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).
        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(1000);
        cc.setRetryStrategy(new DefaultRetryStrategy());
        ots = new AsyncClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(), cc);
        
        try {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }
        createTable(ots);

        try {
            DeleteTableRequest request = new DeleteTableRequest(tableNameWithAutoPK);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }
        createTableWithAutoPK(ots);

        Thread.sleep(3000);
    }

    @After
    public void after() {
        ots.shutdown();
    }

    @Test
    public void testDefaultLongValue() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testDefaultLongValue"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

        // 首次写入，price值加10
        rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
        rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
        rowUpdateChange.addReturnColumn("price");

        Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
        UpdateRowResponse r = resp.get();
        Row rRow = r.getRow();
        System.out.println("更新结果（add price）: start");
        System.out.println(rRow);
        System.out.println("更新结果（add price）: end");
        // 读取数据

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        criteria.addColumnsToGet("price");
        GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
        Row row = getRowResponse.getRow();
        assertEquals(1, row.getColumn("price").size());
        assertEquals(10L, row.getColumn("price").get(0).getValue().asLong());
    }

    @Test
    public void testReturnColumnNames() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testReturnColumnNames"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // 一行单个原子加
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn("price");
            rowUpdateChange.increment(new Column("price-2", ColumnValue.fromLong(100)));

            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
            UpdateRowResponse r = resp.get();
            Row rRow = r.getRow();
            System.out.println("更新结果（add price）: start");
            System.out.println(rRow);
            System.out.println("更新结果（add price）: end");
            assertEquals(1, rRow.getColumn("price").size());
            assertEquals(10L, rRow.getColumn("price").get(0).getValue().asLong());
            assertEquals(0, rRow.getColumn("price-2").size());

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10L, row.getColumn("price").get(0).getValue().asLong());
            assertEquals(0, row.getColumn("price-2").size());
        }

        // 一行2个原子加
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn("price");
            rowUpdateChange.increment(new Column("price-2", ColumnValue.fromLong(100)));
            rowUpdateChange.addReturnColumn("price-2");

            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
            UpdateRowResponse r = resp.get();
            Row rRow = r.getRow();
            System.out.println("更新结果（add price）: start");
            System.out.println(rRow);
            System.out.println("更新结果（add price）: end");
            assertEquals(1, rRow.getColumn("price").size());
            assertEquals(20L, rRow.getColumn("price").get(0).getValue().asLong());
            assertEquals(1, rRow.getColumn("price-2").size());
            assertEquals(200L, rRow.getColumn("price-2").get(0).getValue().asLong());

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            criteria.addColumnsToGet("price-2");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(20L, row.getColumn("price").get(0).getValue().asLong());
            assertEquals(1, row.getColumn("price-2").size());
            assertEquals(200L, row.getColumn("price-2").get(0).getValue().asLong());
        }

        // returnColumnNames增加非原子加列，报错
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn("price");
            rowUpdateChange.put(new Column("price-3", ColumnValue.fromLong(100)));
            rowUpdateChange.addReturnColumn("price-3");

            try {
                Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
                UpdateRowResponse r = resp.get();
            } catch (Exception e){
                // 捕获return column设置异常
                System.out.println(e);
            }

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            criteria.addColumnsToGet("price-3");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(20L, row.getColumn("price").get(0).getValue().asLong());
            assertEquals(0, row.getColumn("price-3").size());
        }

        // returnColumnNames增加非更新列，报错
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn("price");
            rowUpdateChange.addReturnColumn("price-4");

            try {
            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
            UpdateRowResponse r = resp.get();
            } catch (Exception e){
                // 捕获return column设置异常
                System.out.println(e);
            }

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(20L, row.getColumn("price").get(0).getValue().asLong());
            assertEquals(0, row.getColumn("price-4").size());
            System.out.println("GetRow: ");
            System.out.println(row);
        }

        // returnType为RT_PK，returnColumnNames非空，报错
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.put(new Column("price-5", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_PK);
            rowUpdateChange.addReturnColumn("price-5");

            try {
                Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
                UpdateRowResponse r = resp.get();
            } catch (Exception e){
                // 捕获return column设置异常
                System.out.println(e);
            }

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(0, row.getColumn("price-5").size());
            System.out.println("GetRow: ");
            System.out.println(row);
        }
    }

    @Test
    public void testNormalUpdateReturnPK() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testNormalUpdateReturnPK001"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

        // 首次写入，price值加10
        rowUpdateChange.put(new Column("price", ColumnValue.fromLong(20)));
        rowUpdateChange.put(new Column("price", ColumnValue.fromLong(10)));
        rowUpdateChange.setReturnType(ReturnType.RT_PK);

        Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
        UpdateRowResponse r = resp.get();
        Row rRow = r.getRow();
        System.out.println("更新结果（add price）: start");
        System.out.println(rRow);
        System.out.println("更新结果（add price）: end");
        // 读取数据

        // 读一行
        {
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            //criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            System.out.println("GetRow: ");
            System.out.println(rRow);
        }

        {
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableName);

            // 设置起始主键
            PrimaryKeyBuilder primaryKeyBuilder1 = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder1.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("a"));
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder1.build());

            // 设置结束主键
            primaryKeyBuilder1 = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder1.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("z"));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder1.build());

            rangeRowQueryCriteria.setMaxVersions(1);

            System.out.println("GetRange的结果为:");
            while (true) {
                GetRangeResponse getRangeResponse = ots.getRange(new GetRangeRequest(rangeRowQueryCriteria), null).get();
                for (Row row : getRangeResponse.getRows()) {
                    System.out.println(row);
                }

                // 若nextStartPrimaryKey不为null, 则继续读取.
                if (getRangeResponse.getNextStartPrimaryKey() != null) {
                    rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
                } else {
                    break;
                }
            }
        }
    }

    @Test
    public void testAddSameKey() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testAddSameKey"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        long num = 500;
        for (long i = 0; i < num; i++) {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(1)));
            rowUpdateChange.setReturnType(ReturnType.RT_PK);
            //rowUpdateChange.addReturnColumn("price");

            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
            UpdateRowResponse r = resp.get();
            Row rRow = r.getRow();
            if (i % 20 == 0) {
                System.out.println(rRow);
            }
        }

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        criteria.addColumnsToGet("price");
        GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
        Row row = getRowResponse.getRow();
        assertEquals(1, row.getColumn("price").size());
        assertEquals(num, row.getColumn("price").get(0).getValue().asLong());
    }

    @Test
    public void testBatchWrite() throws Exception {
        long rowNum = 10;
        long colNum = 3;
        for (long j = 0; j < colNum; j++) {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            for (long i = 0; i < rowNum; i++) {
                // 原子加
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + i));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
                rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(1)));
                rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
                rowUpdateChange.addReturnColumn("price");
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            Future<BatchWriteRowResponse> resp = ots.batchWriteRow(batchWriteRowRequest, null);
            BatchWriteRowResponse r = resp.get();
            Map<String, List<BatchWriteRowResponse.RowResult>> rs = r.getRowStatus();
            System.out.println("================");
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : rs.entrySet()) {
                for (BatchWriteRowResponse.RowResult rowRes : entry.getValue()) {
                    Row row = rowRes.getRow();
                    System.out.println(row);
                }
            }
        }

        // 读一行
        {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 1));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(colNum, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testAtomicAddWithCondition() throws Exception {
        long rowNum = 2;
        long colNum = 2;
        for (long j = 0; j < colNum; j++) {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            for (long i = 0; i < rowNum; i++) {
                // 原子加
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + i));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
                rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(1)));
                rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
                rowUpdateChange.addReturnColumn("price");

                Condition condition = new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST);
                ColumnCondition columnCondition = new SingleColumnValueCondition(
                        "price",
                        SingleColumnValueCondition.CompareOperator.EQUAL,
                        ColumnValue.fromLong(0));
                condition.setColumnCondition(columnCondition);
                rowUpdateChange.setCondition(condition);
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            Future<BatchWriteRowResponse> resp = ots.batchWriteRow(batchWriteRowRequest, null);
            BatchWriteRowResponse r = resp.get();
            Map<String, List<BatchWriteRowResponse.RowResult>> rs = r.getRowStatus();
            System.out.println("================");
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : rs.entrySet()) {
                for (BatchWriteRowResponse.RowResult rowRes : entry.getValue()) {
                    Row row = rowRes.getRow();
                    System.out.println("error: " + rowRes.getError() + ", " + row);
                }
            }
        }

        // 读一行
        {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 1));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(1, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testRowDeleteAndReplace() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testRowDeleteAndReplace"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10)));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn("price");

            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
            UpdateRowResponse r = resp.get();
            Row rRow = r.getRow();
            System.out.println("更新结果（add price）: start");
            System.out.println(rRow);
            System.out.println("更新结果（add price）: end");
            // 读取数据

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10L, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testAutoPkWrite() throws Exception {
        {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            // 先写入第一个pk自增行和原子加
            PrimaryKey rPKs;
            {
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(autoPk1, PrimaryKeyValue.fromString("autoPK"));
                primaryKeyBuilder.addPrimaryKeyColumn(autoPk2, PrimaryKeyValue.AUTO_INCREMENT);
                PrimaryKey primaryKey = primaryKeyBuilder.build();

                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableNameWithAutoPK, primaryKey);
                //rowUpdateChange.put(new Column("price", ColumnValue.fromLong(1)));
                rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(1)));
                rowUpdateChange.setReturnType(ReturnType.RT_PK);
                //rowUpdateChange.addReturnColumn("price");

                Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
                UpdateRowResponse r = resp.get();
                Row rRow = r.getRow();

                rPKs = rRow.getPrimaryKey();
                System.out.println(rPKs);
            }

            // pk自增与原子加
            {
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(rPKs.getPrimaryKeyColumn(0));
                primaryKeyBuilder.addPrimaryKeyColumn(rPKs.getPrimaryKeyColumn(1));
                System.out.println(rPKs.getPrimaryKeyColumn(0));
                System.out.println(rPKs.getPrimaryKeyColumn(1));

                PrimaryKey primaryKey = primaryKeyBuilder.build();

                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableNameWithAutoPK, primaryKey);
                rowUpdateChange.put(new Column("item", ColumnValue.fromLong(12)));
                rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(15)));
                rowUpdateChange.addReturnColumn("price");
                rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);

                Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
                rowUpdateChange.setCondition(condition);

                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            Future<BatchWriteRowResponse> resp = ots.batchWriteRow(batchWriteRowRequest, null);
            BatchWriteRowResponse r = resp.get();
            Map<String, List<BatchWriteRowResponse.RowResult>> rs = r.getRowStatus();
            System.out.println("Update的结果为");
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : rs.entrySet()) {
                for (BatchWriteRowResponse.RowResult rowRes : entry.getValue()) {
                    Row row = rowRes.getRow();
                    System.out.println("error: " + rowRes.getError() + ", " + row);
                    assertEquals(16L, row.getColumn("price").get(0).getValue().asLong());
                }
            }
            System.out.println("");
        }

        {
            RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(tableNameWithAutoPK);

            // 设置起始主键
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(autoPk1, PrimaryKeyValue.fromString("autoPK"));
            primaryKeyBuilder.addPrimaryKeyColumn(autoPk2, PrimaryKeyValue.fromLong(0));
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

            // 设置结束主键
            primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(autoPk1, PrimaryKeyValue.fromString("autoPK"));
            primaryKeyBuilder.addPrimaryKeyColumn(autoPk2, PrimaryKeyValue.fromLong(Long.MAX_VALUE));
            rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

            rangeRowQueryCriteria.setMaxVersions(1);

            System.out.println("GetRange的结果为:");
            while (true) {
                GetRangeResponse getRangeResponse = ots.getRange(new GetRangeRequest(rangeRowQueryCriteria), null).get();
                for (Row row : getRangeResponse.getRows()) {
                    System.out.println(row);
                }

                // 若nextStartPrimaryKey不为null, 则继续读取.
                if (getRangeResponse.getNextStartPrimaryKey() != null) {
                    rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
                } else {
                    break;
                }
            }
        }
    }

    @Test
    public void testBatchWriteError() throws Exception {
        long rowNum = 2;
        long colNum = 3;
        for (long j = 0; j < colNum; j++) {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            for (long i = 0; i < rowNum; i++) {
                // 原子加
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 0));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
                //rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(1)));
                rowUpdateChange.increment(new Column("price_" + i, ColumnValue.fromLong(1)));
                //rowUpdateChange.put(new Column("price", ColumnValue.fromLong(12)));
                rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
                rowUpdateChange.addReturnColumn("price_" + i);
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            Future<BatchWriteRowResponse> resp = ots.batchWriteRow(batchWriteRowRequest, null);
            BatchWriteRowResponse r = resp.get();
            Map<String, List<BatchWriteRowResponse.RowResult>> rs = r.getRowStatus();
            System.out.println("================");
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : rs.entrySet()) {
                for (BatchWriteRowResponse.RowResult rowRes : entry.getValue()) {
                    Row row = rowRes.getRow();
                    System.out.println("error: " + rowRes.getError() + ", " + row);
                }
            }
        }

        // 读一行
        {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 1));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            System.out.println(row);
            //assertEquals(1, row.getColumn("price").size());
            //assertEquals(colNum, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testBatchWriteError2() throws Exception {
        /*
        long rowNum = 2;
        long colNum = 3;
        for (long j = 0; j < colNum; j++) {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

            for (long i = 0; i < rowNum; i++) {
                // 原子加
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 0));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

                if (i % 2 == 0) {
                    Condition condition = new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST);
                    ColumnCondition columnCondition = new SingleColumnValueCondition(
                            "price",
                            SingleColumnValueCondition.CompareOperator.EQUAL,
                            ColumnValue.fromLong(0));
                    condition.setColumnCondition(columnCondition);
                    rowUpdateChange.setCondition(condition);
                    rowUpdateChange.put(new Column("price", ColumnValue.fromLong(j)));
                } else {
                    Condition condition = new Condition(RowExistenceExpectation.IGNORE);
                    ColumnCondition columnCondition = new SingleColumnValueCondition(
                            "price",
                            SingleColumnValueCondition.CompareOperator.EQUAL,
                            ColumnValue.fromLong(j));
                    condition.setColumnCondition(columnCondition);
                    rowUpdateChange.setCondition(condition);
                    rowUpdateChange.put(new Column("price", ColumnValue.fromLong(j * rowNum)));
                }

                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            Future<BatchWriteRowResponse> resp = ots.batchWriteRow(batchWriteRowRequest, null);
            BatchWriteRowResponse r = resp.get();
            Map<String, List<BatchWriteRowResponse.RowResult>> rs = r.getRowStatus();
            System.out.println("================");
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : rs.entrySet()) {
                for (BatchWriteRowResponse.RowResult rowRes : entry.getValue()) {
                    Row row = rowRes.getRow();
                    System.out.println("error: " + rowRes.getError() + ", " + row);
                }
            }
        }

        // 读一行
        {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("bw_" + 1));
            PrimaryKey primaryKey = primaryKeyBuilder.build();

            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            System.out.println(row);
            //assertEquals(1, row.getColumn("price").size());
            //assertEquals(colNum, row.getColumn("price").get(0).getValue().asLong());
        }
        */
    }

    @Test
    public void testMultiThread() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testMultiThread"));
        final PrimaryKey primaryKey = primaryKeyBuilder.build();

        List<Thread> threads = new ArrayList<Thread>();
        long numThreads = 20;
        final long num = 25;
        for (long t = 1; t <= numThreads; t++) {
            final long tid = t;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (long i = 0; i < num; i++) {
                        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

                        rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(tid)));
                        rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
                        rowUpdateChange.addReturnColumn("price");

                        try {
                            Future<UpdateRowResponse> resp = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null);
                            UpdateRowResponse r = resp.get();
                            Row rRow = r.getRow();
                            //if (i % 20 == 0) {
                                System.out.println("thread id: " + tid + ", " + rRow);
                            //}
                        } catch (Exception e) {
                            System.out.println("write error");
                        }
                    }
                }
            });
            threads.add(th);
        }
        for (Thread th : threads) {
            th.start();
        }
        for (Thread th : threads) {
            th.join();
        }

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        criteria.addColumnsToGet("price");
        GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
        Row row = getRowResponse.getRow();
        assertEquals(1, row.getColumn("price").size());
        assertEquals(num * (numThreads * (numThreads + 1) / 2), row.getColumn("price").get(0).getValue().asLong());
    }

    @Test
    public void testDefaultDoubleValue() throws Exception {
        /*
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testDefaultDoubleValue"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

        // 首次写入，price值加1
        rowUpdateChange.increment(new Column("price", ColumnValue.fromDouble(10.54)));

        ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

        // 读取数据

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        criteria.addColumnsToGet("price");
        GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
        Row row = getRowResponse.getRow();
        assertEquals(1, row.getColumn("price").size());
        assertEquals(10.54, row.getColumn("price").get(0).getValue().asDouble(), 0.0000001);
        */
    }

    @Test
    public void testLongValueIncrement() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testLongValueIncrement"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加10
            rowUpdateChange.put(new Column("price", ColumnValue.fromLong(10)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10L, row.getColumn("price").get(0).getValue().asLong());
        }
        // 递增
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(515)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(525L, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testLongValueDecrease() throws Exception {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testLongValueDecrease"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.put(new Column("price", ColumnValue.fromLong(10)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10L, row.getColumn("price").get(0).getValue().asLong());
        }
        // 递增
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(-3)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(7L, row.getColumn("price").get(0).getValue().asLong());
        }
    }

    @Test
    public void testDoubleValueIncrement() throws Exception {
        /*
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testDoubleValueIncrement"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.put(new Column("price", ColumnValue.fromDouble(10.9)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10.9, row.getColumn("price").get(0).getValue().asLong(), 0.000001);
        }
        // 递增
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.increment(new Column("price", ColumnValue.fromDouble(1.8)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(12.7, row.getColumn("price").get(0).getValue().asDouble(), 0.0000001);
        }
        */
    }

    @Test
    public void testDoubleValueDecrease() throws Exception {
        /*
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testDoubleValueDecrease"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.put(new Column("price", ColumnValue.fromDouble(10.1)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(10.1, row.getColumn("price").get(0).getValue().asDouble(), 0.000001);
        }
        // 递增
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);

            // 首次写入，price值加1
            rowUpdateChange.increment(new Column("price", ColumnValue.fromDouble(-3.2)));

            ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();

            // 读一行
            SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
            // 设置读取最新版本
            criteria.setMaxVersions(1);
            criteria.addColumnsToGet("price");
            GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
            Row row = getRowResponse.getRow();
            assertEquals(1, row.getColumn("price").size());
            assertEquals(6.9, row.getColumn("price").get(0).getValue().asDouble(), 0.000001);
        }
        */
    }

    @Test
    public void testBatchIncrement() throws Exception {
        // 第一次写入，作为默认值
        {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
            // 构造rowUpdateChange
            {
                PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pk3Builder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-1"));
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk3Builder.build());
                // 添加一些列
                rowUpdateChange.increment(new Column("long", ColumnValue.fromLong(100)));
                rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
                rowUpdateChange.addReturnColumn("long");
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            // 构造rowUpdateChange
            {
                PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pk3Builder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-2"));
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk3Builder.build());
                // 添加一些列
                rowUpdateChange.increment(new Column("long", ColumnValue.fromLong(300)));
                rowUpdateChange.setReturnType(ReturnType.RT_PK);
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            BatchWriteRowResponse response = ots.batchWriteRow(batchWriteRowRequest, null).get();
            assertTrue(response.isAllSucceed());
            Map<String, List<BatchWriteRowResponse.RowResult>> res =  response.getRowStatus();
            for (Map.Entry<String, List<BatchWriteRowResponse.RowResult>> entry : res.entrySet()) {
                for (BatchWriteRowResponse.RowResult rs : entry.getValue()) {
                    System.out.println("结果：");
                    System.out.println(rs.getRow());
                }
            }
        }

        // 查询检查结果
        {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            for (int i = 1; i < 3; i++) {
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-" + i));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                multiRowQueryCriteria.addRow(primaryKey);
            }

            multiRowQueryCriteria.setMaxVersions(1);
            multiRowQueryCriteria.addColumnsToGet("long");

            BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
            batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

            BatchGetRowResponse batchGetRowResponse = ots.batchGetRow(batchGetRowRequest, null).get();

            assertTrue(batchGetRowResponse.isAllSucceed());

            BatchGetRowResponse.RowResult rowResult = batchGetRowResponse.getSucceedRows().get(0);
            assertEquals("testBatchIncrement-1", rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString());
            assertEquals(100L, rowResult.getRow().getColumn("long").get(0).getValue().asLong());

            rowResult = batchGetRowResponse.getSucceedRows().get(1);
            assertEquals("testBatchIncrement-2", rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString());
            assertEquals(300L, rowResult.getRow().getColumn("long").get(0).getValue().asLong());
        }

        // 第二次写入，增加值
        {
            BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
            // 构造rowUpdateChange
            {
                PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pk3Builder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-1"));
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk3Builder.build());
                // 添加一些列
                rowUpdateChange.increment(new Column("long", ColumnValue.fromLong(10)));
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            // 构造rowUpdateChange
            {
                PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                pk3Builder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-2"));
                RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, pk3Builder.build());
                // 添加一些列
                rowUpdateChange.increment(new Column("long", ColumnValue.fromLong(-50)));
                batchWriteRowRequest.addRowChange(rowUpdateChange);
            }

            BatchWriteRowResponse response = ots.batchWriteRow(batchWriteRowRequest, null).get();
            assertTrue(response.isAllSucceed());
        }

        // 查询检查结果
        {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            for (int i = 1; i < 3; i++) {
                PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBatchIncrement-" + i));
                PrimaryKey primaryKey = primaryKeyBuilder.build();
                multiRowQueryCriteria.addRow(primaryKey);
            }

            multiRowQueryCriteria.setMaxVersions(1);
            multiRowQueryCriteria.addColumnsToGet("long");

            BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
            batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

            BatchGetRowResponse batchGetRowResponse = ots.batchGetRow(batchGetRowRequest, null).get();

            assertTrue(batchGetRowResponse.isAllSucceed());

            BatchGetRowResponse.RowResult rowResult = batchGetRowResponse.getSucceedRows().get(0);
            assertEquals("testBatchIncrement-1", rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString());
            assertEquals(110L, rowResult.getRow().getColumn("long").get(0).getValue().asLong());

            rowResult = batchGetRowResponse.getSucceedRows().get(1);
            assertEquals("testBatchIncrement-2", rowResult.getRow().getPrimaryKey().getPrimaryKeyColumn(0).getValue().asString());
            assertEquals(250L, rowResult.getRow().getColumn("long").get(0).getValue().asLong());
        }
    }
}

