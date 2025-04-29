package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;

import java.util.ArrayList;
import java.util.Iterator;

public class GlobalIndexSample {

    /**
     * In this example, a table is created named globalIndexSampleMainTable with two primary keys, which are pk1 and pk2 respectively. There are also two predefined columns, namely col1 and col2.
     * Additionally, an index table named globalIndexSampleIndexTable is created, with the index column being col1 and the attribute column being col2.
     */
    private static final String TABLE_NAME = "globalIndexSampleMainTable";
    private static final String INDEX_NAME = "globalIndexSampleIndexTable";
    private static final String INDEX2_NAME = "globalIndexSampleIndexTable2";
    private static final String PRIMARY_KEY_NAME_1 = "pk1";
    private static final String PRIMARY_KEY_NAME_2 = "pk2";
    private static final String DEFINED_COL_NAME_1 = "col1";
    private static final String DEFINED_COL_NAME_2 = "col2";
    private static final String ADD_COL_NAME_0 = "c0";
    private static final String ADD_COL_NAME_1 = "c1";

    public static void main(String[] args) {
        ServiceSettings serviceSettings = ServiceSettings.load();
        final String endPoint = serviceSettings.getOTSEndpoint();
        final String accessId = serviceSettings.getOTSAccessKeyId();
        final String accessKey = serviceSettings.getOTSAccessKeySecret();
        final String instanceName = serviceSettings.getOTSInstanceName();

        SyncClient client = new SyncClient(endPoint, accessId, accessKey, instanceName);
        try {

            //deleteTable(client);
            // Create a table with two PK columns, pk1 and pk2, and two predefined columns, col1 and col2.
            // where the index is built on cok1, and the attribute column of the index table is col2
            createTable(client);

            System.out.println("create table succeeded.");

            // Wait for the table to load.
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Get the list of tables, excluding index tables.
            ListTableResponse response = client.listTable();
            System.out.println("All tables: ");
            for (String tableName : response.getTableNames()) {
                System.out.println(tableName);
            }

            // Describe the main table and index table
            describeTable(client, TABLE_NAME);
            describeTable(client, INDEX_NAME);

            // Write a row of data to the main table
            PrimaryKey pk = putRow(client);

            // Read from the main table and verify that the data has been written.
            getRowFromMainTable(client, pk);

            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // Read from the index table, verify that the index has been established
            scanFromIndex(client);

            // Update the row just written to the main table.
            updateRow(client, pk);

            // Create an index table 2 on the main table alone. The primary key (PK) of index table 2 is col2 and pk2, and the attribute columns are empty.
            createIndex(client);

            // Read from the index table and verify that the data update in the main table has been synchronized to the index table.
            scanFromIndex(client);

            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Read from index table 2 and verify that the data in the main table has been synchronized to index table 2.
            scanFromIndex2(client);

            // Delete the rows written in the main table
            deleteRow(client, pk);

            // Read from index table 1 and verify that the index in the index table has been deleted.
            scanFromIndex(client);

            // Read from index table 2 and verify that the index in the index table has been deleted.
            scanFromIndex2(client);

            // Add predefined columns to the table
            addDefCol(client);
            // View the table schema after adding predefined columns
            describeTable(client, TABLE_NAME);
            try {
                Thread.sleep(1 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            updateRow(client, pk);
            scanFromIndex(client);
            // Delete the pre-defined column
            delDefCol(client);
            // View the table schema after deleting the predefined column
            describeTable(client, TABLE_NAME);

            // Delete the index table
            deleteIndex(client);

            // Delete the main table
            deleteTable(client);

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            // For security reasons, drop table cannot be set as default here. If you need to drop a table, please enable it manually.
            // deleteTable(client);
        }
        client.shutdown();
    }

    private static void addDefCol(SyncClient client) {
        ArrayList<DefinedColumnSchema> defCols = new ArrayList<DefinedColumnSchema>();

        AddDefinedColumnRequest request = new AddDefinedColumnRequest();
            request.setTableName(TABLE_NAME);
            request.addDefinedColumn(ADD_COL_NAME_0, DefinedColumnType.STRING);
            request.addDefinedColumn(ADD_COL_NAME_1, DefinedColumnType.STRING);
            client.addDefinedColumn(request);
    }

    private static void delDefCol(SyncClient client) {
        DeleteDefinedColumnRequest request = new DeleteDefinedColumnRequest();
        request.setTableName(TABLE_NAME);
        request.addDefinedColumn(ADD_COL_NAME_0);
        request.addDefinedColumn(ADD_COL_NAME_1);
        client.deleteDefinedColumn(request);
    }

    private static void createTable(SyncClient client) {
        TableMeta tableMeta = new TableMeta(TABLE_NAME);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_1, PrimaryKeyType.STRING));
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema(PRIMARY_KEY_NAME_2, PrimaryKeyType.INTEGER));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_1, DefinedColumnType.STRING));
        tableMeta.addDefinedColumn(new DefinedColumnSchema(DEFINED_COL_NAME_2, DefinedColumnType.INTEGER));

        int timeToLive = -1; // The expiration time of the data, in seconds, -1 means never expire. The expiration time of the main table data with indexed tables must be -1.
        int maxVersions = 1; // The maximum number of versions to save, the maximum version number of the table with index must be 1

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
        // Construct the primary key
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromString("chengdu"));
        primaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(123));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TABLE_NAME, primaryKey);

        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_1, ColumnValue.fromString("abc")));
        rowPutChange.addColumn(new Column(DEFINED_COL_NAME_2, ColumnValue.fromLong(456)));

        PutRowResponse response = client.putRow(new PutRowRequest(rowPutChange));
        // Print out the consumed CU
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
        // Read a row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, pk);
        criteria.setMaxVersions(1);

        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        System.out.println("read a row, result: ");
        System.out.println(row);
    }
    private static void scanFromIndex(SyncClient client) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX_NAME);

        // Set the start primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Set the end primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("getRange table1 result:");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                System.out.println(row);
            }

            // If nextStartPrimaryKey is not null, continue reading.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }
    }

    private static void scanFromIndex2(SyncClient client) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX2_NAME);

        // Set the start primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_2, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Set the end primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        System.out.println("getRange table2 result:");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                System.out.println(row);
            }

            // If nextStartPrimaryKey is not null, continue reading.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }
    }
}
