package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.Iterator;

public class GlobalIndexSample {

    /**
     * 本示例中建立一张表,名为globalIndexSampleMainTable,两个主键, 主键分别为pk1，pk2. 两个预定义列，分别为col1, col2
     * 另外建立一张索引表，名为globalIndexSampleIndexTable, 索引列为col1, 属性列为col2
     */
    private static final String TABLE_NAME = "globalIndexSampleMainTable";
    private static final String INDEX_NAME = "globalIndexSampleIndexTable";
    private static final String INDEX2_NAME = "globalIndexSampleIndexTable2";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";
    private static final String DEFINED_COL_NAME_1 = "col1";
    private static final String DEFINED_COL_NAME_2 = "col2";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        try {

            //deleteTable(client);
            // 建表，表中两列PK，pk1, pk2，另外两列预定义列，col1, col2
            // 其中在cok1上建立索引，索引表的属性列为col2
            createTable(client);

            System.out.println("create table succeeded.");

            // 等待表load完毕.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 获取表列表，其中不不包含索引表
            ListTableResponse response = client.listTable();
            System.out.println("All tables：");
            for (String tableName : response.getTableNames()) {
                System.out.println(tableName);
            }

            // describe主表及索引表
            describeTable(client, TABLE_NAME);
            describeTable(client, INDEX_NAME);

            // 向主表中写入一行数据
            PrimaryKey pk = putRow(client);

            // 从主表中读取，验证该数据已经被写入
            getRowFromMainTable(client, pk);

            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 从索引表中读取，验证索引已经被建立
            scanFromIndex(client);

            // 更新刚才中主表中写入的行
            updateRow(client, pk);

            // 单独在主表上创建索引表2，索引表2的PK为col2, pk2, 属性列为空
            createIndex(client);

            // 从索引表中读取，验证主表中的数据更新已经同步到了索引表
            scanFromIndex(client);

            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 从索引表2中读取，验证主表中的数据已经同步到了索引表2
            scanFromIndex2(client);

            // 将主表中写入的行删除
            deleteRow(client, pk);

            // 从索引表1中读取，验证索引表中的索引已经被删除
            scanFromIndex(client);

            // 从索引表2中读取，验证索引表中的索引已经被删除
            scanFromIndex2(client);

            // 删除索引表
            deleteIndex(client);

            // 删除主表
            deleteTable(client);

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
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_1, DefinedColumnType.STRING));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_2, DefinedColumnType.INTEGER));

        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 带索引表的主表数据过期时间必须为-1
        int maxVersions = 1; // 保存的最大版本数, 带索引表的请表最大版本数必须为1

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        ArrayList<IndexMeta> indexMetas = new ArrayList<IndexMeta>();
        IndexMeta indexMeta = new IndexMeta(INDEX_NAME);
        indexMeta.addPrimaryKeyColumn(DEFINED_COL_NAME_1);
        indexMeta.addDefinedColumn(DEFINED_COL_NAME_2);
        indexMetas.add(indexMeta);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions, indexMetas);

        client.createTable(request);
    }

    private static void createIndex(SyncClient client) {
        IndexMeta indexMeta = new IndexMeta(INDEX2_NAME);
        indexMeta.addPrimaryKeyColumn(DEFINED_COL_NAME_2);
        indexMeta.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2);
        CreateIndexRequest request = new CreateIndexRequest(TABLE_NAME, indexMeta, false);
        client.createIndex(request);
    }

    private static void describeTable(SyncClient client, String tableName) {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        DescribeTableResponse response = client.describeTable(request);
        TableMeta tableMeta = response.getTableMeta();
        for (PrimaryKeySchema pk : tableMeta.getPrimaryKeyList()) {
            System.out.println(pk.getName());
            System.out.println(pk.getType());
        }
        for (DefinedColumnSchema defCol : tableMeta.getDefinedColumnsList()) {
            System.out.println(defCol.getName());
            System.out.println(defCol.getType());
        }
    }

    private static void deleteIndex(SyncClient client) {
        DeleteIndexRequest request = new DeleteIndexRequest(TABLE_NAME, INDEX_NAME);
        client.deleteIndex(request);
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static PrimaryKey putRow(SyncClient client) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(123));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_1, ColumnValue.fromString("abc")));
        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_2, ColumnValue.fromLong(456)));

        PutRowResponse response = client.putRow(new PutRowRequest(rowPutChange));
        // 打印出消耗的CU
        CapacityUnit  cu = response.getConsumedCapacity().getCapacityUnit();
        System.out.println("Read CapacityUnit:" + cu.getReadCapacityUnit());
        System.out.println("Write CapacityUnit:" + cu.getWriteCapacityUnit());
        return primaryKey;
    }


    private static void deleteRow(SyncClient client, PrimaryKey pk) {
        RowDeleteChange rowDeleteChange = new RowDeleteChange(TABLE_NAME, pk);

        client.deleteRow(new DeleteRowRequest(rowDeleteChange));
    }

    private static void updateRow(SyncClient client, PrimaryKey pk) {
        RowUpdateChange rowUpdateChange = new RowUpdateChange(TABLE_NAME, pk);

        rowUpdateChange.put(new Column(DEFINED_COL_NAME_1, ColumnValue.fromString("def")));

        client.updateRow(new UpdateRowRequest(rowUpdateChange));
    }

    private static void getRowFromMainTable(SyncClient client, PrimaryKey pk)
    {
        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        criteria.setMaxVersions(1);

        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("读取主表完毕, 结果为: ");
        System.out.println(row);
    }
    private static void scanFromIndex(SyncClient client) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX_NAME);

        // 设置起始主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 设置结束主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("扫描索引表1的结果为:");
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

    private static void scanFromIndex2(SyncClient client) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX2_NAME);

        // 设置起始主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_2, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 设置结束主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("扫描索引表2的结果为:");
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
}
