package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.BulkExportRequest;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DataOperationSample {

    /**
     * 本示例中建立一张表,名为sampleTable,只含有一个主键, 主键名为pk.
     */
    private static final String TABLE_NAME = "sampleTable";
    private static final String PRIMARY_KEY_NAME = "pk";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // 建表
            deleteTable(client);
            createTable(client);

            // 等待表load完毕.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow
            putRow(client, "pkValue");

            // getRow
            getRow(client, "pkValue");

            // updateRow
            updateRow(client, "pkValue");

            // 使用condition递增一列
            updateRowWithCondition(client, "pkValue");

            // getRow
            getRow(client, "pkValue");

            // 再写入两行
            putRow(client, "aaa");
            putRow(client, "bbb");

            increment(client, "pkValue");

            // getRange
            getRange(client, "a", "z");

            // 使用iterator进行getRange
            getRangeByIterator(client, "a", "z");

            batchWriteRow(client);
            
            batchGetRow(client);

            getRange(client, "a", "z");

        } catch (TableStoreException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("请求失败，详情：" + e.getMessage());
        } finally {
            // 为了安全，这里不能默认删表，如果需要删表，需用户自己手动打开
            // deleteTable(client);
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME, PrimaryKeyType.STRING));

        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        int maxVersions = 1; // 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void putRow(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        //加入一些属性列
        long ts = System.currentTimeMillis();
        rowPutChange.addColumn(new Column("price", ColumnValue.fromLong(5120), ts));

        client.putRow(new PutRowRequest(rowPutChange));
    }

    private static void updateRow(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);

        // 更新一些列
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }

        // 删除某列的某一版本
        rowUpdateChange.deleteColumn("Col10", 1465373223000L);

        // 删除某一列
        rowUpdateChange.deleteColumns("Col11");

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    private static void deleteRow(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, primaryKey);

        client.deleteRow(new DeleteRowRequest(rowDeleteChange));
    }

    private static void increment(SyncClient client, String pkValue) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);
        rowUpdateChange.increment(new Column("price", ColumnValue.fromLong(10000L)));
        rowUpdateChange.setReturnType(ReturnType.RT_PK);

        UpdateRowResponse response = client.updateRow(new UpdateRowRequest(rowUpdateChange));
        Row row = response.getRow();
        System.out.println("更新结果（add value = 10000）: start");
        System.out.println(row);
        System.out.println("更新结果（add value = 10000）: end");
    }

    private static void batchWriteRow(SyncClient client) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        // 构造rowPutChange1
        PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk1"));
        RowPutChange rowPutChange1 = new RowPutChange(TABLE_NAME, pk1Builder.build());
        // 添加一些列
        for (int i = 0; i < 10; i++) {
            rowPutChange1.addColumn(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowPutChange1);

        // 构造rowPutChange2
        PrimaryKeyBuilder pk2Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk2"));
        RowPutChange rowPutChange2 = new RowPutChange(TABLE_NAME, pk2Builder.build());
        // 添加一些列
        for (int i = 0; i < 10; i++) {
            rowPutChange2.addColumn(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowPutChange2);

        // 构造rowUpdateChange
        PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk3"));
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk3Builder.build());
        // 添加一些列
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }
        // 删除一列
        rowUpdateChange.deleteColumns("Col10");
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowUpdateChange);

        // 构造rowDeleteChange
        PrimaryKeyBuilder pk4Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk4"));
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk4Builder.build());
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowDeleteChange);

        // 构造increment
        PrimaryKeyBuilder primaryKeyBuilderInc = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilderInc.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pkValue"));
        PrimaryKey primaryKeyInc = primaryKeyBuilderInc.build();
        RowUpdateChange rowUpdateChangeInc = new RowUpdateChange(TABLE_NAME, primaryKeyInc);
        rowUpdateChangeInc.increment(new Column("price", ColumnValue.fromLong(20000L)));
        rowUpdateChangeInc.setReturnType(ReturnType.RT_PK);
        batchWriteRowRequest.addRowChange(rowUpdateChangeInc);

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        System.out.println("是否全部成功:" + response.isAllSucceed());
        if (!response.isAllSucceed()) {
            for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                System.out.println("失败的行:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                System.out.println("失败原因:" + rowResult.getError());
            }
            /**
             * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
             * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
             */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
        }
    }

    private static void getRow(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("读取行完毕, 结果为: ");
        System.out.println(row);

        // 设置读取某些列
        criteria.addColumnsToGet("Col0");
        getRowResponse = client.getRow(new GetRowRequest(criteria));
        row = getRowResponse.getRow();

        System.out.println("读取列（col0）完毕, 结果为: ");
        System.out.println(row);
    }

    private static void getRowWithFilter(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);

        // 设置过滤器, 当Col0的值为0时返回该行.
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
        // 如果不存在Col0这一列, 也不返回.
        singleColumnValueFilter.setPassIfMissing(false);
        // 只判断最新版本
        singleColumnValueFilter.setLatestVersionsOnly(true);

        criteria.setFilter(singleColumnValueFilter);

        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("读取完毕, 结果为: ");
        System.out.println(row);
    }

    private static void batchGetRow(SyncClient client) {
        MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(TABLE_NAME);
        // 加入10个要读取的行
        for (int i = 0; i < 10; i++) {
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString("pk" + i));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            multiRowQueryCriteria.addRow(primaryKey);
        }
        // 添加条件
        multiRowQueryCriteria.setMaxVersions(1);
        multiRowQueryCriteria.addColumnsToGet("Col0");
        multiRowQueryCriteria.addColumnsToGet("Col1");
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("Col0",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(0));
        singleColumnValueFilter.setPassIfMissing(false);
        multiRowQueryCriteria.setFilter(singleColumnValueFilter);

        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        // batchGetRow支持读取多个表的数据, 一个multiRowQueryCriteria对应一个表的查询条件, 可以添加多个multiRowQueryCriteria.
        batchGetRowRequest.addMultiRowQueryCriteria(multiRowQueryCriteria);

        BatchGetRowResponse batchGetRowResponse = client.batchGetRow(batchGetRowRequest);

        System.out.println("是否全部成功:" + batchGetRowResponse.isAllSucceed());
        if (!batchGetRowResponse.isAllSucceed()) {
            for (BatchGetRowResponse.RowResult rowResult : batchGetRowResponse.getFailedRows()) {
                System.out.println("失败的行:" + batchGetRowRequest.getPrimaryKey(rowResult.getTableName(), rowResult.getIndex()));
                System.out.println("失败原因:" + rowResult.getError());
            }

            /**
             * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
             * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
             */
            BatchGetRowRequest retryRequest = batchGetRowRequest.createRequestForRetry(batchGetRowResponse.getFailedRows());
        }
    }

    // 通过Condition实现乐观锁机制, 递增一列.
    private static void updateRowWithCondition(SyncClient client, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, primaryKey);
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        long col0Value = row.getLatestColumn("Col0").getValue().asLong();

        // 条件更新Col0这一列, 使列值+1
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, primaryKey);
        Condition condition = new Condition(RowExistenceExpectation.EXPECT_EXIST);
        ColumnCondition columnCondition = new SingleColumnValueCondition("Col0", SingleColumnValueCondition.CompareOperator.EQUAL, ColumnValue.fromLong(col0Value));
        condition.setColumnCondition(columnCondition);
        rowUpdateChange.setCondition(condition);
        rowUpdateChange.put(new Column("Col0", ColumnValue.fromLong(col0Value + 1)));

        try {
            client.updateRow(new UpdateRowRequest(rowUpdateChange));
        } catch (TableStoreException ex) {
            System.out.println(ex.toString());
        }
    }

    private static void getRange(SyncClient client, String startPkValue, String endPkValue) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(TABLE_NAME);

        // 设置起始主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(startPkValue));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // 设置结束主键
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(endPkValue));
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("GetRange的结果为:");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
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

    private static void getRangeByIterator(SyncClient client, String startPkValue, String endPkValue) {
        RangeIteratorParameter rangeIteratorParameter = new RangeIteratorParameter(TABLE_NAME);

        // 设置起始主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(startPkValue));
        rangeIteratorParameter.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // 设置结束主键
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(endPkValue));
        rangeIteratorParameter.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeIteratorParameter.setMaxVersions(1);

        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);

        System.out.println("使用Iterator进行GetRange的结果为:");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println(row);
        }
    }

    private static void bulkImport(SyncClient client, String start, String end){
        // create bulkImportRequest
        BulkImportRequest bulkImportRequest = new BulkImportRequest(TABLE_NAME);

        // create rowChanges
        List<RowChange> rowChanges = new ArrayList<RowChange>();
        for (Integer i = Integer.valueOf(start); i <= Integer.valueOf(end); i++){
            PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            primaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(i)));
            PrimaryKey primaryKey = primaryKeyBuilder.build();
            RowPutChange rowChange = new RowPutChange(TABLE_NAME,primaryKey);
            rowChange.addColumn(new Column("DC1", ColumnValue.fromString(i.toString())));
            rowChange.addColumn(new Column("DC2", ColumnValue.fromString(i.toString())));
            rowChange.addColumn(new Column("DC3", ColumnValue.fromString(i.toString())));
            rowChanges.add(rowChange);
        }

        bulkImportRequest.addRowChanges(rowChanges);
        // get bulkImportResponse
        BulkImportResponse bulkImportResponse = client.bulkImport(bulkImportRequest);
    }

    private static void bulkExport(SyncClient client, String start, String end){
        // create startPrimaryKey
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(start)));
        PrimaryKey startPrimaryKey = startPrimaryKeyBuilder.build();

        // create endPrimaryKey
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn("pk", PrimaryKeyValue.fromString(String.valueOf(end)));
        PrimaryKey endPrimaryKey = endPrimaryKeyBuilder.build();

        // create bulkExportRequest
        BulkExportRequest bulkExportRequest = new BulkExportRequest();
        // create bulkExportQueryCriteria
        BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(TABLE_NAME);

        bulkExportQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKey);
        bulkExportQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKey);
        bulkExportQueryCriteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);
        bulkExportQueryCriteria.addColumnsToGet("pk");
        bulkExportQueryCriteria.addColumnsToGet("DC1");
        bulkExportQueryCriteria.addColumnsToGet("DC2");

        bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);
        // get bulkExportResponse
        BulkExportResponse bulkExportResponse = client.bulkExport(bulkExportRequest);
    }
}
