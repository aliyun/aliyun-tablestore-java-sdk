package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

public class ServerSideEncryptionSample {

    /**
     * In this example, three tables are created, each with only one primary key, and the primary key name is "pk".
     * One table is named disableSseSampleTable, and server-side encryption is disabled for this table.
     * One table is named kmsServiceSampleTable, and server-side encryption is enabled for this table using KMS's service master key.
     * One table is named byokSampleTable, and server-side encryption is enabled for this table using KMS's customer managed master key.
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
            // Create a table with server-side encryption disabled
            deleteTableIfExist(client, TABLE_NAME_DISABLE);
            createTableDisableSse(client, TABLE_NAME_DISABLE);

            // Create a table with the server-side encryption feature (service master key) enabled
            deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE);
            createTableKmsService(client, TABLE_NAME_KMS_SERVICE);

            // Create a table with server-side encryption (user master key) enabled
            deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE);
            createTableByok(client, TABLE_NAME_BYOK, keyId, roleArn);

            // View the properties of two tables
            describeTable(client, TABLE_NAME_DISABLE);
            describeTable(client, TABLE_NAME_KMS_SERVICE);
            describeTable(client, TABLE_NAME_BYOK);

            // Wait for the table to load.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Write one row of data to each of the two tables
            putRow(client, TABLE_NAME_DISABLE, "pkValue");
            putRow(client, TABLE_NAME_KMS_SERVICE, "pkValue");
            putRow(client, TABLE_NAME_BYOK, "pkValue");

            // Read the row data from each of the two tables.
            getRow(client, TABLE_NAME_DISABLE, "pkValue");
            getRow(client, TABLE_NAME_KMS_SERVICE, "pkValue");
            getRow(client, TABLE_NAME_BYOK, "pkValue");

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
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
        // Disable server-side encryption
        SSESpecification sseSpec = new SSESpecification(false);
        createTable(client, tableName, sseSpec);
    }

    private static void createTableKmsService(SyncClient client, String tableName) {
        // Enable server-side encryption, using the service master key of KMS
        // Make sure that the KMS service has been activated in the corresponding region.
        SSESpecification sseSpec = new SSESpecification(true, SSEKeyType.SSE_KMS_SERVICE);
        createTable(client, tableName, sseSpec);
    }

    private static void createTableByok(SyncClient client, String tableName, String keyId, String roleArn) {
        // Enable server-side encryption, using the user's main key in KMS
        // It is necessary to ensure that the keyId is valid and not disabled, and that the roleArn has been granted temporary access permissions for this keyId.
        SSESpecification sseSpec = new SSESpecification(true, SSEKeyType.SSE_BYOK, keyId, roleArn);
        createTable(client, tableName, sseSpec);
    }

    private static void describeTable(SyncClient client, String tableName) {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        DescribeTableResponse response = client.describeTable(request);

        TableMeta tableMeta = response.getTableMeta();
        System.out.println("table name:" + tableMeta.getTableName());
        SSEDetails sseDetails = response.getSseDetails();
        if (sseDetails.isEnable()) {
            System.out.println("enable encryption: yes");
            System.out.println("encryption key type: " + sseDetails.getKeyType().toString());
            System.out.println("encryption key id: " + sseDetails.getKeyId());
            if (sseDetails.getKeyType() == SSEKeyType.SSE_BYOK) {
                System.out.println("sse row arn: " + sseDetails.getRoleArn());
            }
        } else {
            System.out.println("enable encryption: no");
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
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);

        // Add some property columns
        long ts = System.currentTimeMillis();
        rowPutChange.addColumn(new Column("price", ColumnValue.fromLong(5120), ts));

        client.putRow(new PutRowRequest(rowPutChange));
    }

    private static void getRow(SyncClient client, String tableName, String pkValue) {
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME, PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // Read one row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        // Set to read the latest version
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read a row, result: ");
        System.out.println(row);
    }
}
