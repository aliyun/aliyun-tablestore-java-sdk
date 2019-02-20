package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

/**
 * Created by yizheng on 16/4/28.
 */
public class TableOperationSample {

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
            // 创建表
            createTable(client);

            // list table查看表的列表
            listTable(client);

            // 查看表的属性
            describeTable(client);

            // 更新表的属性
            updateTable(client);

            // update table完之后查看表的属性
            describeTable(client);

            // list table查看表的列表
            listTable(client);
        } catch (TableStoreException e) {
            System.err.println("操作失败，详情：" + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("请求失败，详情：" + e.getMessage());
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME, PrimaryKeyType.STRING));

        int timeToLive = -1; // 数据的过期时间, 单位秒, -1代表永不过期. 假如设置过期时间为一年, 即为 365 * 24 * 3600.
        int maxVersions = 3; // 保存的最大版本数, 设置为3即代表每列上最多保存3个最新的版本.

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static void updateTable(SyncClient client) {
        int timeToLive = -1;
        int maxVersions = 5; //更新最大版本数为5.

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        UpdateTableRequest request = new UpdateTableRequest(TABLE_NAME);
        request.setTableOptionsForUpdate(tableOptions);

        client.updateTable(request);
    }

    private static void describeTable(SyncClient client) {
        DescribeTableRequest request = new DescribeTableRequest(TABLE_NAME);
        DescribeTableResponse response = client.describeTable(request);

        TableMeta tableMeta = response.getTableMeta();
        System.out.println("表的名称：" + tableMeta.getTableName());
        System.out.println("表的主键：");
        for (PrimaryKeySchema primaryKeySchema : tableMeta.getPrimaryKeyList()) {
            System.out.println(primaryKeySchema);
        }
        TableOptions tableOptions = response.getTableOptions();
        System.out.println("表的TTL:" + tableOptions.getTimeToLive());
        System.out.println("表的MaxVersions:" + tableOptions.getMaxVersions());
        ReservedThroughputDetails reservedThroughputDetails = response.getReservedThroughputDetails();
        System.out.println("表的预留读吞吐量："
                + reservedThroughputDetails.getCapacityUnit().getReadCapacityUnit());
        System.out.println("表的预留写吞吐量："
                + reservedThroughputDetails.getCapacityUnit().getWriteCapacityUnit());
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void listTable(SyncClient client) {
        ListTableResponse response = client.listTable();
        System.out.println("表的列表如下：");
        for (String tableName : response.getTableNames()) {
            System.out.println(tableName);
        }
    }
}
