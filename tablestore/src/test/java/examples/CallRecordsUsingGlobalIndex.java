package examples;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;

public class CallRecordsUsingGlobalIndex {

    /**
     * Create a call record table which contains two primary key columns. The first primary key is the caller number, and the second primary key is the call occurrence time, both of Integer type.
     * The call record table also contains three predefined columns: callee number, call duration, and base station number, all of Integer type.
     * Additionally, create three index tables. The first one is the callee number index table with the primary key being the callee number and no attribute columns.
     * The second one is the base station call index table with primary keys as the base station number and call occurrence time, and no attribute columns.
     * The third one is the base station call time index table with primary keys as the base station number and call occurrence time, and the attribute column as the call duration.
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
            // Create table
            createTable(client);

            System.out.println("create table success.");

            // Wait for loading to complete
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Write call records to the main table
            putRow(client, 123456, 1532574644, 654321, 60, 1);
            putRow(client, 234567, 1532574714, 765432, 10, 1);
            putRow(client, 234567, 1532574734, 123456, 20, 3);
            putRow(client, 345678, 1532574795, 123456, 5, 2);
            putRow(client, 456789, 1532584054, 345678, 200, 3);

            // Query all the calling bills of number `234567`

            getRangeFromMainTable(client, 234567);

            // Query all the called call records of number `123456`
            getRangeFromIndexTable(client, 123456);

            // Query all bills of base station `2` starting from time `1532574740`
            getRangeFromIndexTable(client,2, 1532574740);

            // Query the call duration of all call records for base station `3` from time `1532574861` to `1532584054`.
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

            // Drop table
            deleteTable(client);

        } catch (TableStoreException e) {
            System.err.println("operation failed, detail: " + e.getMessage());
            System.err.println("Request ID:" + e.getRequestId());
        } catch (ClientException e) {
            System.err.println("request failed, detail: " + e.getMessage());
        } finally {
            // For security reasons, table deletion cannot be set as default here. If you need to delete a table, please enable it manually.
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


        int timeToLive = -1; // The expiration time of the data, in seconds, -1 means never expires. The expiration time of the main table data with indexed tables must be -1.
        int maxVersions = 1; // The maximum number of versions to be saved, and the maximum number of versions for tables with index must be 1.

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
        // Construct the primary key
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

        // Construct the primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(0));
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Construct the primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strNum = String.format("%d", cellNumber);
        System.out.println("cell number " + strNum + " query result:");
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

    private static void getRangeFromIndexTable(SyncClient client, long cellNumber) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX0_NAME);

        // Construct the primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Construct the primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_1, PrimaryKeyValue.fromLong(cellNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strNum = String.format("%d", cellNumber);
        System.out.println("CalledNumber query result of cell number " + strNum + ":");
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

    private static void getRangeFromIndexTable(SyncClient client,
                                               long baseStationNumber,
                                               long startTime) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX1_NAME);

        // Construct the primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Construct the primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.INF_MAX);
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        System.out.println("BaseStation " + strBaseStationNum + " query result from " + strStartTime + ":");
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

    private static void getRowFromIndexAndMainTable(SyncClient client,
                                                    long baseStationNumber,
                                                    long startTime,
                                                    long endTime,
                                                    String colName) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(INDEX1_NAME);

        // Construct the primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Construct the primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(endTime));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        String strEndTime = String.format("%d", endTime);

        System.out.println("Duration query result of BaseStation " + strBaseStationNum + " from " + strStartTime + " to " + strEndTime + ":");
        while (true) {
            GetRangeResponse getRangeResponse = client.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                PrimaryKey curIndexPrimaryKey = row.getPrimaryKey();
                PrimaryKeyColumn mainCalledNumber = curIndexPrimaryKey.getPrimaryKeyColumn(PRIMARY_KEY_NAME_1);
                PrimaryKeyColumn callStartTime = curIndexPrimaryKey.getPrimaryKeyColumn(PRIMARY_KEY_NAME_2);
                PrimaryKeyBuilder mainTablePKBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
                mainTablePKBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, mainCalledNumber.getValue());
                mainTablePKBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, callStartTime.getValue());
                PrimaryKey mainTablePK = mainTablePKBuilder.build(); // Construct the primary key of the main table

                // Reverse lookup main table
                SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TABLE_NAME, mainTablePK);
                criteria.addColumnsToGet(colName); // Read the "call duration" column of the main table
                // Set to read the latest version
                criteria.setMaxVersions(1);
                GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
                Row mainTableRow = getRowResponse.getRow();

                System.out.println(mainTableRow);
            }

            // If nextStartPrimaryKey is not null, continue reading.
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

        // Construct the primary key
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(startTime));
        startPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // Construct the primary key
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn(DEFINED_COL_NAME_3, PrimaryKeyValue.fromLong(baseStationNumber));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_2, PrimaryKeyValue.fromLong(endTime));
        endPrimaryKeyBuilder.addPrimaryKeyColumn(PRIMARY_KEY_NAME_1, PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        // Set the read column
        rangeRowQueryCriteria.addColumnsToGet(colName);

        rangeRowQueryCriteria.setMaxVersions(1);

        String strBaseStationNum = String.format("%d", baseStationNumber);
        String strStartTime = String.format("%d", startTime);
        String strEndTime = String.format("%d", endTime);


        System.out.println("Duration query result of BaseStation " + strBaseStationNum + " from " + strStartTime + " to " + strEndTime + ":");
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
