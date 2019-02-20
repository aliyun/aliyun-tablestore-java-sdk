package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;

public class CallRecordsUsingGlobalIndex {

    /**
     * 建立通话记录表，其中包含两列主键，第一列主键为主叫号码，第二列主键为通话发生时间，类型均为Interger
     * 通话记录表中包含三列预定义列，分别为被叫号码，通话时长，基站号码，类型均为Integer
     * 另外建立三张索引表，第一张为被叫号码索引表，主键为被叫号码，没有属性列
     * 第二张为基站话单索引表，主键为基站号码，通话发生时间，没有属性列
     * 第三张为基站话单时间索引表，主键为基站号码，通话发生时间，属性列为通话时长
     */
    private static final String TABLE_NAME = "CallRecordTable";
    private static final String INDEX0_NAME = "IndexOnBeCalledNumber";
    private static final String INDEX1_NAME = "IndexOnBaseStation1";
    private static final String INDEX2_NAME = "IndexOnBaseStation2";
    private static final String PRIMARY_KEY_NAME_1 = "CellNumber";
    private static final String PRIMARY_KEY_NAME_2 = "StartTime";
    private static final String DEFINED_COL_NAME_1 = "CalledNumber";
    private static final String DEFINED_COL_NAME_2 = "Duration";
    private static final String DEFINED_COL_NAME_3 = "BaseStationNumber";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey, instanceName);

        try {

            //deleteTable(client);
            // 建表
            createTable(client);

            System.out.println("建表成功.");

            // 等待加载完成
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 向主表中写入通话记录
            putRow(client, 123456, 1532574644, 654321, 60, 1);
            putRow(client, 234567, 1532574714, 765432, 10, 1);
            putRow(client, 234567, 1532574734, 123456, 20, 3);
            putRow(client, 345678, 1532574795, 123456, 5, 2);
            putRow(client, 456789, 1532584054, 345678, 200, 3);

            // 查询号码`234567`的所有主叫话单

            getRangeFromMainTable(client, 234567);

            // 查询号码`123456`的所有被叫话单
            getRangeFromIndexTable(client, 123456);

            // 查询基站`2`从时间`1532574740`开始的所有话单
            getRangeFromIndexTable(client,2, 1532574740);

            // 查询基站`3`从时间`1532574861`到`1532584054`的所有通话记录的通话时长
            getRowFromIndexAndMainTable(client,
                    3,
                    1532574861,
                    1532584054,
                    DEFINED_COL_NAME_2);
            getRangeFromIndexTable(client,
                    3,
                    1532574861,
                    1532584054,
                    DEFINED_COL_NAME_2);

            // 删表
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
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.INTEGER));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_1, DefinedColumnType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_2, DefinedColumnType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_3, DefinedColumnType.INTEGER));


        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 带索引表的主表数据过期时间必须为-1
        int maxVersions = 1; // 保存的最大版本数, 带索引表的请表最大版本数必须为1

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        ArrayList<IndexMeta> indexMetas = new ArrayList<IndexMeta>();
        IndexMeta indexMeta0 = new IndexMeta(INDEX0_NAME);
        indexMeta0.addPrimaryKeyColumn(DEFINED_COL_NAME_1);
        indexMetas.add(indexMeta0);
        IndexMeta indexMeta1 = new IndexMeta(INDEX1_NAME);
        indexMeta1.addPrimaryKeyColumn(DEFINED_COL_NAME_3);
        indexMeta1.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2);
        indexMetas.add(indexMeta1);
        IndexMeta indexMeta2 = new IndexMeta(INDEX2_NAME);
        indexMeta2.addPrimaryKeyColumn(DEFINED_COL_NAME_3);
        indexMeta2.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2);
        indexMeta2.addDefinedColumn(DEFINED_COL_NAME_2);
        indexMetas.add(indexMeta2);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions, indexMetas);

        client.createTable(request);
    }


    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void putRow(SyncClient client, long pk1, long pk2, long def1, long def2, long def3) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromLong(pk1));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(pk2));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_1, ColumnValue.fromLong(def1)));
        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_2, ColumnValue.fromLong(def2)));
        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_3, ColumnValue.fromLong(def3)));

        client.putRow(new PutRowRequest(rowPutChange));
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

    private static void getRangeFromMainTable(SyncClient client, long cellNumber)
    {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(TABLE_NAME);

        // 构造主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 构造主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strNum = String.format("%d", cellNumber);
        System.out.println("号码" + strNum + "的所有主叫话单:");
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

    private static void getRangeFromIndexTable(SyncClient client, long cellNumber) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX0_NAME);

        // 构造主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 构造主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strNum = String.format("%d", cellNumber);
        System.out.println("号码" + strNum + "的所有被叫话单:");
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

    private static void getRangeFromIndexTable(SyncClient client,
                                               long baseStationNumber,
                                               long startTime) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX1_NAME);

        // 构造主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 构造主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        System.out.println("基站" + strBaseStationNum + "从时间" + strStartTime + "开始的所有被叫话单:");
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

    private static void getRowFromIndexAndMainTable(SyncClient client,
                                                    long baseStationNumber,
                                                    long startTime,
                                                    long endTime,
                                                    String colName) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX1_NAME);

        // 构造主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 构造主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(endTime));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        String strEndTime = String.format("%d", endTime);

        System.out.println("基站" + strBaseStationNum + "从时间" + strStartTime + "到" + strEndTime + "的所有话单通话时长:");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                PrimaryKey curIndexPrimaryKey = row.getPrimaryKey();
                PrimaryKeyColumn mainCalledNumber = curIndexPrimaryKey.getPrimaryKeyColumn(PRIMARY_KEY_NAME_1);
                PrimaryKeyColumn callStartTime = curIndexPrimaryKey.getPrimaryKeyColumn(PRIMARY_KEY_NAME_2);
                PrimaryKeyBuilder mainTablePKBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                mainTablePKBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, mainCalledNumber.getValue());
                mainTablePKBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, callStartTime.getValue());
                PrimaryKey mainTablePK = mainTablePKBuilder.build(); // 构造主表PK

                // 反查主表
                SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, mainTablePK);
                criteria.addColumnsToGet(colName); // 读取主表的"通话时长"列
                // 设置读取最新版本
                criteria.setMaxVersions(1);
                GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
                Row mainTableRow = getRowResponse.getRow();

                System.out.println(mainTableRow);
            }

            // 若nextStartPrimaryKey不为null, 则继续读取.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }
    }

    private static void getRangeFromIndexTable(SyncClient client,
                                               long baseStationNumber,
                                               long startTime,
                                               long endTime,
                                               String colName) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX2_NAME);

        // 构造主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 构造主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(endTime));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        // 设置读取列
        rangeRowQueryCriteria.addColumnsToGet(colName);

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        String strEndTime = String.format("%d", endTime);


        System.out.println("基站" + strBaseStationNum + "从时间" + strStartTime + "到" + strEndTime + "的所有话单通话时长:");
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
