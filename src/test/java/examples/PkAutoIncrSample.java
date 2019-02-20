package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

import java.util.Iterator;

public class PkAutoIncrSample {

    /**
     * 本示例中建立一张表,名为sampleTable,两个主键, 主键分别为pk1，pk2.
     */
    private static final String TABLE_NAME = "sampleTable_pk";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // 建表
            createTable(client);

            System.out.println("create table succeeded.");

            // 等待表load完毕.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // putRow
            PrimaryKey pk = putRow(client);

            System.out.println("put row succeeded,pk:" + pk.toString());

            // getRow
            getRow(client, pk);

            // updateRow
            updateRow(client, pk);

            getRowWithFilter(client, pk);

            // 使用condition递增一列
            updateRowWithCondition(client, pk);

            // getRow
            getRow(client, pk);

            // 再写入两行
            putRow(client);
            putRow(client);

            // getRange
            getRange(client, "a", "z");

            // 使用iterator进行getRange
            getRangeByIterator(client, "a", "z");

            batchWriteRow(client);

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
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));

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

    private static PrimaryKey putRow(SyncClient client) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);
        rowPutChange.setReturnType(ReturnType.RT_PK);

        //加入一些属性列
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 3; j++) {
                rowPutChange.addColumn(new Column("Col" + i, ColumnValue.fromLong(j), ts + j));
            }
        }

        PutRowResponse response = client.putRow(new PutRowRequest(rowPutChange));
        // 打印出消耗的CU
        CapacityUnit  cu = response.getConsumedCapacity().getCapacityUnit();
        System.out.println("Read CapacityUnit:" + cu.getReadCapacityUnit());
        System.out.println("Write CapacityUnit:" + cu.getWriteCapacityUnit());

        // 打印出返回的PK列
        PrimaryKey pk = response.getRow().getPrimaryKey();
        System.out.println("PrimaryKey:" + pk.toString());

        return pk;
    }

    private static void updateRow(SyncClient client, PrimaryKey pk) {
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk);

        // 更新一些列
        for (int i = 0; i < 10; i++) {
            rowUpdateChange.put(new Column("Col" + i, ColumnValue.fromLong(i)));
        }

        // 删除某列的某一版本
        rowUpdateChange.deleteColumn("Col10", 1465373223000L);

        // 删除某一列
        rowUpdateChange.deleteColumns("Col11");
        rowUpdateChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_EXIST));

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    private static void deleteRow(SyncClient client, PrimaryKey pk) {
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk);

        client.deleteRow(new DeleteRowRequest(rowDeleteChange));
    }

    private static void batchWriteRow(SyncClient client) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();

        // 构造rowPutChange1
        PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk1Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        RowPutChange rowPutChange1 = new RowPutChange(TABLE_NAME, pk1Builder.build());
        rowPutChange1.setReturnType(ReturnType.RT_PK);
        // 添加一些列
        rowPutChange1.addColumn(new Column("Column_0", ColumnValue.fromLong(99)));

        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowPutChange1);

        // 构造rowPutChange2
        PrimaryKeyBuilder pk2Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk2Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);
        RowPutChange rowPutChange2 = new RowPutChange(TABLE_NAME, pk2Builder.build());
        rowPutChange2.setReturnType(ReturnType.RT_PK);
        // 添加一些列
        rowPutChange2.addColumn(new Column("Column_0", ColumnValue.fromLong(100)));

        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowPutChange2);

        // 构造rowUpdateChange
        PrimaryKeyBuilder pk3Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk3Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.AUTO_INCREMENT);

        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk3Builder.build());
        rowUpdateChange.setReturnType(ReturnType.RT_PK);
        // 添加一列
        rowUpdateChange.put(new Column("Column_0", ColumnValue.fromLong(101)));

        // 删除一列
        rowUpdateChange.deleteColumns("Column_1");
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowUpdateChange);

        // 构造rowDeleteChange
        PrimaryKeyBuilder pk4Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("Hangzhou"));
        pk4Builder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(1));
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk4Builder.build());
        rowDeleteChange.setReturnType(ReturnType.RT_PK);
        // 添加到batch操作中
        batchWriteRowRequest.addRowChange(rowDeleteChange);

        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        System.out.println("是否全部成功:" + response.isAllSucceed());
        if (!response.isAllSucceed()) {
            for (BatchWriteRowResponse.RowResult rowResult : response.getFailedRows()) {
                System.out.println("失败的行:" + batchWriteRowRequest.getRowChange(rowResult.getTableName(), rowResult.getIndex()).getPrimaryKey());
                System.out.println("失败原因:" + rowResult.getError());
            }
            /*
             * 可以通过createRequestForRetry方法再构造一个请求对失败的行进行重试.这里只给出构造重试请求的部分.
             * 推荐的重试方法是使用SDK的自定义重试策略功能, 支持对batch操作的部分行错误进行重试. 设定重试策略后, 调用接口处即不需要增加重试代码.
             */
            BatchWriteRowRequest retryRequest = batchWriteRowRequest.createRequestForRetry(response.getFailedRows());
        } else {
            for (BatchWriteRowResponse.RowResult rowResult : response.getSucceedRows()) {
                PrimaryKey pk = rowResult.getRow().getPrimaryKey();
                System.out.println("Return PK:" + pk.jsonize());
            }
        }
    }

    private static void getRow(SyncClient client, PrimaryKey pk) {
        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("读取完毕, 结果为: ");
        System.out.println(row);

        // 设置读取某些列
        criteria.addColumnsToGet("Col0");
        getRowResponse = client.getRow(new GetRowRequest(criteria));
        row = getRowResponse.getRow();

        System.out.println("读取完毕, 结果为: ");
        System.out.println(row);
    }

    private static void getRowWithFilter(SyncClient client, PrimaryKey pk) {
        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
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

    // 通过Condition实现乐观锁机制, 递增一列.
    private static void updateRowWithCondition(SyncClient client, PrimaryKey pk) {
        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        long col0Value = row.getLatestColumn("Col0").getValue().asLong();

        // 条件更新Col0这一列, 使列值+1
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk);
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
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(startPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // 设置结束主键
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(endPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
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
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(startPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeIteratorParameter.setInclusiveStartPrimaryKey(primaryKeyBuilder.build());

        // 设置结束主键
        primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString(endPkValue));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeIteratorParameter.setExclusiveEndPrimaryKey(primaryKeyBuilder.build());

        rangeIteratorParameter.setMaxVersions(1);

        Iterator<Row> iterator = client.createRangeIterator(rangeIteratorParameter);

        System.out.println("使用Iterator进行GetRange的结果为:");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            System.out.println(row);
        }
    }

}
