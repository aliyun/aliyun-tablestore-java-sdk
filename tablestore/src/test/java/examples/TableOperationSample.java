package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;

/**
 * Created by yizheng on 16/4/28.
 */
public class TableOperationSample {

    /**
     * In this example, a table named sampleTable is created, which contains only one primary key, and the primary key name is pk.
     */
    private static final String TABLE_NAME = "sampleTable";
    private static final String PRIMARY_KEY_NAME = "pk";
    private static final String DEFINED_COLUMN_NAME = "defined_column";

    public static void main(String[] args) {
        ServiceSettings serviceSettings = ServiceSettings.load();
        final String endPoint = serviceSettings.getOTSEndpoint();
        final String accessId = serviceSettings.getOTSAccessKeyId();
        final String accessKey = serviceSettings.getOTSAccessKeySecret();
        final String instanceName = serviceSettings.getOTSInstanceName();

        SyncClient client = new SyncClient(endPoint, accessId, accessKey,
                instanceName);

        try {
            if (client.listTable().getTableNames().contains(TABLE_NAME)) {
                deleteTable(client);
            }

            // Create table
            createTable(client);

            // List tables to view the list of tables
            listTable(client);

            // View table properties
            describeTable(client);

            // Update the table's properties
            updateTable(client);

            deleteTable(client);
            // List tables to view the table list.
            listTable(client);
        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        // Configure table name, primary key columns, and predefined columns
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME, PrimaryKeyType.STRING));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COLUMN_NAME, DefinedColumnType.STRING));

        int timeToLive = -1; // The expiration time of the data, in seconds. -1 means never expires. If the expiration time is set to one year, it would be 365 * 24 * 3600.
        int maxVersions = 1; // The maximum number of versions to save, setting it to 1 means that at most 1 latest version is saved for each column.

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        tableOptions.setAllowUpdate(false);   // Whether to allow data update.
        tableOptions.setUpdateFullRow(false); // Whether the entire row must be updated together when updating data.

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        client.createTable(request);
    }

    private static void updateTable(SyncClient client) {
        int timeToLive = -1;
        int maxVersions = 5; // Update the maximum version number to 5.

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);
        tableOptions.setAllowUpdate(true); // Update allows data update.

        UpdateTableRequest request = new UpdateTableRequest(TABLE_NAME);
        request.setTableOptionsForUpdate(tableOptions);

        UpdateTableResponse response = client.updateTable(request);
        TableOptions responseTableOptions = response.getTableOptions();
        System.out.println("after updateTable, TTL:" + responseTableOptions.getTimeToLive());
        System.out.println("after updateTable, MaxVersions:" + responseTableOptions.getMaxVersions());
        System.out.println("after updateTable, AllowUpdate:" + responseTableOptions.getAllowUpdate());
        System.out.println("after updateTable, UpdateFullRow:" + responseTableOptions.getUpdateFullRow());
    }

    private static void describeTable(SyncClient client) {
        DescribeTableRequest request = new DescribeTableRequest(TABLE_NAME);
        DescribeTableResponse response = client.describeTable(request);

        TableMeta tableMeta = response.getTableMeta();
        System.out.println("table name: " + tableMeta.getTableName());
        System.out.println("pk: ");
        for (PrimaryKeySchema primaryKeySchema : tableMeta.getPrimaryKeyList()) {
            System.out.println(primaryKeySchema);
        }
        TableOptions tableOptions = response.getTableOptions();
        System.out.println("TTL:" + tableOptions.getTimeToLive());
        System.out.println("MaxVersions:" + tableOptions.getMaxVersions());
        System.out.println("AllowUpdate:" + tableOptions.getAllowUpdate());
        System.out.println("UpdateFullRow:" + tableOptions.getUpdateFullRow());
        ReservedThroughputDetails reservedThroughputDetails = response.getReservedThroughputDetails();
        System.out.println("Reserved ReadCapacity: "
                + reservedThroughputDetails.getCapacityUnit().getReadCapacityUnit());
        System.out.println("Reserved WriteCapacity: "
                + reservedThroughputDetails.getCapacityUnit().getWriteCapacityUnit());
    }

    private static void deleteTable(SyncClient client) {
        DeleteTableRequest request = new DeleteTableRequest(TABLE_NAME);
        client.deleteTable(request);
    }

    private static void listTable(SyncClient client) {
        ListTableResponse response = client.listTable();
        System.out.println("table list: ");
        for (String tableName : response.getTableNames()) {
            System.out.println(tableName);
        }
    }
}
