package examples;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.*;

public class LocalTransactionSample {
    /**
     * 本示例中建立一张表,名为sampleTable,两个主键, 主键分别为pk1，pk2.
     */
    private static final String TABLE_NAME = "LocalTransactionSample";
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

            String txnId = startTxn(client);

            putRow(txnId, client);

            // commit txn
            client.commitTransaction(new CommitTransactionRequest(txnId));
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

        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        int maxVersions = 1; // 保存的最大版本数, 设置为1即代表每列上最多保存一个版本(保存最新的版本).

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static String startTxn(SyncClient client) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        StartLocalTransactionRequest request = new StartLocalTransactionRequest(TABLE_NAME, primaryKey);
        StartLocalTransactionResponse response = client.startLocalTransaction(request);
        return response.getTransactionID();
    }

    private static void putRow(String txnId, SyncClient client) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(10000));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        //加入一些属性列
        rowPutChange.addColumn(new Column("Col", ColumnValue.fromLong(1099)));

        PutRowRequest request = new PutRowRequest(rowPutChange);
        request.setTransactionId(txnId);
        client.putRow(request);
    }
}
