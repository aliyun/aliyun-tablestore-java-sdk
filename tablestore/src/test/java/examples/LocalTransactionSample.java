package examples;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.*;

public class LocalTransactionSample {
    /**
     * In this example, a table named sampleTable is created with two primary keys, namely pk1 and pk2.
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
            // Create table
            createTable(client);

            System.out.println("create table succeeded.");

            // Wait for the table to load.
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
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            // For security reasons, drop table cannot be set as default here. If you need to drop the table, please enable it manually.
            // deleteTable(client);
        }
        client.shutdown();
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER));

        int timeToLive = -1; // The expiration time of the data, in seconds. -1 means never expires. If you set the expiration time to one year, it would be 365 * 24 * 3600.
        int maxVersions = 1; // The maximum number of versions to save, setting it to 1 means that at most one version is saved for each column (saving the latest version).

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
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(10000));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        // Add some property columns
        rowPutChange.addColumn(new Column("Col", ColumnValue.fromLong(1099)));

        PutRowRequest request = new PutRowRequest(rowPutChange);
        request.setTransactionId(txnId);
        client.putRow(request);
    }
}
