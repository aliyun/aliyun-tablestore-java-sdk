package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

public class ServerSideEncryptionSample {

    /**
     * 本示例中建立三张表,都只含有一个主键, 主键名为pk。
     * 一张表名为disableSseSampleTable，该表关闭服务器端加密功能
     * 一张表名为kmsServiceSampleTable, 该表开启服务器端加密功能，且使用KMS的服务主密钥
     * 一张表名为byokSampleTable, 该表开启服务器端加密功能，且使用KMS的用户主密钥
     */
    private static final String TABLE_NAME_DISABLE = "disableSseSampleTable";
    private static final String TABLE_NAME_KMS_SERVICE = "kmsServiceSampleTable";
    private static final String TABLE_NAME_BYOK = "byokSampleTable";
    private static final String PRIMARY_KEY_NAME = "pk";

    public static void main(String[] args) {
        final String endPoint = "";
        final String accessId = "";
        final String accessKey = "";
        final String instanceName = "";

        final String keyId = "";
        final String roleArn = "";

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            // 创建关闭服务器端加密功能的表
            deleteTableIfExist(client, TABLE_NAME_DISABLE);
            createTableDisableSse(client, TABLE_NAME_DISABLE);

            // 创建开启服务器端加密功能(服务主秘钥)的表
            deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE);
            createTableKmsService(client, TABLE_NAME_KMS_SERVICE);

            // 创建开启服务器端加密功能(用户主秘钥)的表
            deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE);
            createTableByok(client, TABLE_NAME_BYOK, keyId, roleArn);

            // 查看两张表的属性
            describeTable(client, TABLE_NAME_DISABLE);
            describeTable(client, TABLE_NAME_KMS_SERVICE);
            describeTable(client, TABLE_NAME_BYOK);

            // 等待表load完毕.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 两张表各写入一行数据
            putRow(client, TABLE_NAME_DISABLE, "pkValue");
            putRow(client, TABLE_NAME_KMS_SERVICE, "pkValue");
            putRow(client, TABLE_NAME_BYOK, "pkValue");

            // 两张表各读取该行数据
            getRow(client, TABLE_NAME_DISABLE, "pkValue");
            getRow(client, TABLE_NAME_KMS_SERVICE, "pkValue");
            getRow(client, TABLE_NAME_BYOK, "pkValue");

        } catch (TableStoreException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("请求失败，详情：" + e.getMessage());
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client, String tableName, SSESpecification sseSpec) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME, PrimaryKeyType.STRING));

        TableOptions tableOptions = new TableOptions(-1, 1);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setSseSpecification(sseSpec);

        client.createTable(request);
    }

    private static void createTableDisableSse(SyncClient client, String tableName) {
        // 关闭服务器端加密功能
        SSESpecification sseSpec = new SSESpecification(false);
        createTable(client, tableName, sseSpec);
    }

    private static void createTableKmsService(SyncClient client, String tableName) {
        // 打开服务器端加密功能，使用KMS的服务主密钥
        // 需要确保已经在所在区域开通了KMS服务
        SSESpecification sseSpec = new SSESpecification(true, SSEKeyType.SSE_KMS_SERVICE);
        createTable(client, tableName, sseSpec);
    }

    private static void createTableByok(SyncClient client, String tableName, String keyId, String roleArn) {
        // 打开服务器端加密功能，使用KMS的用户主密钥
        // 需要确保keyId合法有效且未被禁用，同时roleArn被授予了临时访问该keyId的权限
        SSESpecification sseSpec = new SSESpecification(true, SSEKeyType.SSE_BYOK, keyId, roleArn);
        createTable(client, tableName, sseSpec);
    }

    private static void describeTable(SyncClient client, String tableName) {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        DescribeTableResponse response = client.describeTable(request);

        TableMeta tableMeta = response.getTableMeta();
        System.out.println("表的名称：" + tableMeta.getTableName());
        SSEDetails sseDetails = response.getSseDetails();
        if (sseDetails.isEnable()) {
            System.out.println("表是否开启服务器端加密功能：是");
            System.out.println("表的加密秘钥类型：" + sseDetails.getKeyType().toString());
            System.out.println("表的加密主密钥id：" + sseDetails.getKeyId());
            if (sseDetails.getKeyType() == SSEKeyType.SSE_BYOK) {
                System.out.println("表的全局资源描述符：" + sseDetails.getRoleArn());
            }
        } else {
            System.out.println("表是否开启服务器端加密功能：否");
        }
    }

    private static void deleteTableIfExist(SyncClient client, String tableName) {
        ListTableResponse response = client.listTable();
        if (response.getTableNames().contains(tableName)) {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            client.deleteTable(request);
        }
    }

    private static void putRow(SyncClient client, String tableName, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);

        //加入一些属性列
        long ts = System.currentTimeMillis();
        rowPutChange.addColumn(new Column("price", ColumnValue.fromLong(5120), ts));

        client.putRow(new PutRowRequest(rowPutChange));
    }

    private static void getRow(SyncClient client, String tableName, String pkValue) {
        // 构造主键
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // 读一行
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // 设置读取最新版本
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("读取行完毕, 结果为: ");
        System.out.println(row);
    }
}
