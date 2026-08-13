package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonUpdateRequest {
    private static final String tableName = "JsonUpdateRequestTest";
    private static final String pk = "pk";
    private AsyncClientInterface ots;
    private static ServiceSettings serviceSettings = ServiceSettings.load();

    public void createTable(AsyncClientInterface ots) throws Exception {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(pk, PrimaryKeyType.STRING);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setMaxVersions(1);
        tableOptions.setTimeToLive(-1);
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        request.setReservedThroughput(new ReservedThroughput(new CapacityUnit(0, 0)));
        request.setTableOptions(tableOptions);

        Future<CreateTableResponse> future = ots.createTable(request, null);
        future.get();
    }

    @Before
    public void setUp() throws Exception {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(1000);
        cc.setRetryStrategy(new DefaultRetryStrategy());
        ots = new AsyncClient(serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(), serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(), cc);

        try {
            DeleteTableRequest request = new DeleteTableRequest(tableName);
            Future<DeleteTableResponse> future = ots.deleteTable(request, null);
            future.get();
        } catch (Exception e) {
            // pass
        }
        createTable(ots);

        Thread.sleep(3000);
    }

    @After
    public void after() {
        ots.shutdown();
    }

    private void verifyColumnValue(UpdateRowResponse response, PrimaryKey primaryKey, String columnName, String expectedValue) throws Exception {
        // Verify return row (after modify)
        Row returnRow = response.getRow();
        assertEquals(1, returnRow.getColumn(columnName).size());
        assertEquals(expectedValue, returnRow.getColumn(columnName).get(0).getValue().asString());

        // Verify by reading the row
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName, primaryKey);
        criteria.setMaxVersions(1);
        criteria.addColumnsToGet(columnName);
        GetRowResponse getRowResponse = ots.getRow(new GetRowRequest(criteria), null).get();
        Row row = getRowResponse.getRow();
        assertEquals(1, row.getColumn(columnName).size());
        assertEquals(expectedValue, row.getColumn(columnName).get(0).getValue().asString());
    }

    @Test
    public void testBasic() throws Exception {
        String columnName = "col";

        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn(pk, PrimaryKeyValue.fromString("testBasicUpdate"));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        // Step 0: Put initial row with col = "1"
        {
            RowPutChange rowPutChange = new RowPutChange(tableName, primaryKey);
            rowPutChange.addColumn(columnName, ColumnValue.fromString("1"));
            ots.putRow(new PutRowRequest(rowPutChange), null).get();
        }

        // Step 1: JSON_SET "$" -> {"k":"v"}
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonSet(columnName, "$", ColumnValue.fromString("{\"k\": \"v\"}"));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"k\":\"v\"}");
        }

        // Step 2: JSON_INSERT "$.a.b" -> [1, 2], autoCreateObject=true
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonInsert(columnName, "$.a.b", ColumnValue.fromString("[1, 2]"), true);
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,2]},\"k\":\"v\"}");
        }

        // Step 3: JSON_REPLACE "$.k" -> 3.14
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonReplace(columnName, "$.k", ColumnValue.fromString("3.14"), false);
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,2]},\"k\":3.14}");
        }

        // Step 4: JSON_REMOVE "$.k"
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonRemove(columnName, "$.k");
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,2]}}");
        }

        // Step 5: JSON_ARRAY_APPEND "$.a.b" -> 2
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonArrayAppend(columnName, "$.a.b", ColumnValue.fromString("2"));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,2,2]}}");
        }

        // Step 6: JSON_ARRAY_INSERT "$.a.b[2]" -> 100
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonArrayInsert(columnName, "$.a.b[2]", ColumnValue.fromString("100"));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,2,100,2]}}");
        }

        // Step 7: JSON_ARRAY_REMOVE "$.a.b" -> remove all elements equal to 2
        {
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            rowUpdateChange.jsonArrayRemove(columnName, "$.a.b", ColumnValue.fromString("2"));
            rowUpdateChange.setReturnType(ReturnType.RT_AFTER_MODIFY);
            rowUpdateChange.addReturnColumn(columnName);

            UpdateRowResponse response = ots.updateRow(new UpdateRowRequest(rowUpdateChange), null).get();
            verifyColumnValue(response, primaryKey, columnName, "{\"a\":{\"b\":[1,100]}}");
        }
    }
}
