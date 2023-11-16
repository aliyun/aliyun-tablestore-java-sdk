package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.google.gson.JsonSyntaxException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class DateTimeTest {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "DateTimeTest";
    private static SyncClientInterface client;
    private static Logger LOG = Logger.getLogger(BatchWriteTest.class.getName());
    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        client = Utils.getOTSInstance();
    }

    @AfterClass
    public static void classAfter() {
        client.shutdown();
    }

    @Before
    public void setup() throws Exception {
        ListTableResponse r = client.listTable();

        for (String table: r.getTableNames()) {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(table);
            client.deleteTable(deleteTableRequest);
            LOG.info("Delete table: " + table);

            Thread.sleep(1000);
        }
    }

    private void CreateTable(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        OTSHelper.createTable(ots, tableName, pk);
        LOG.info("Create table: " + tableName);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }

    @Test
    public void testDateTimeBatchOperation() throws Exception {
        Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("pk0",PrimaryKeyType.STRING);
        pks.put("pk1",PrimaryKeyType.DATETIME);

        CreateTable(client, tableName, pks);

        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        //batchWriteRowRequest.setAtomic(true);

        for(int i = 0; i < 50; i++){
            PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pk1Builder.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromString("pk"+i));
            pk1Builder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromDateTime(ZonedDateTime.of(2021, 11, 11, 11, 11, i%60, 123456000, ZoneId.of("Asia/Shanghai"))));
            RowPutChange rowPutChange = new RowPutChange(tableName,pk1Builder.build());
            rowPutChange.addColumn("col0",ColumnValue.fromDateTime(ZonedDateTime.of(2021, 11, 11, 11, 11, i%60, 123456000, ZoneId.of("Asia/Shanghai"))));
            batchWriteRowRequest.addRowChange(rowPutChange);
        }
        BatchWriteRowResponse response = client.batchWriteRow(batchWriteRowRequest);

        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(tableName);
        criteria.addColumnsToGet("col0");
        criteria.setMaxVersions(1);

        for(int i = 0; i < 50; i++){
            PrimaryKeyBuilder pk1Builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
            pk1Builder.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromString("pk"+i));
            pk1Builder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromDateTime(ZonedDateTime.of(2021, 11, 11, 11, 11, i%60, 123456000, ZoneId.of("Asia/Shanghai"))));
            criteria.addRow(pk1Builder.build());
        }

        batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        BatchGetRowResponse batchGetRowResp = client.batchGetRow(batchGetRowRequest);
        Map<String, List<BatchGetRowResponse.RowResult>> tableBatchGetRowResult = batchGetRowResp.getTableToRowsResult();
        List<BatchGetRowResponse.RowResult> results = tableBatchGetRowResult.get(tableName);
        List<RowChange> rowChanges = batchWriteRowRequest.getRowChange().get(tableName);
        for(int i = 0; i < batchWriteRowRequest.getRowsCount(); i++){
            BatchGetRowResponse.RowResult result = results.get(i);
            assertEquals(result.isSucceed(),true);
            Row reqRow = result.getRow();
            RowPutChange rowPutChange = (RowPutChange)(rowChanges.get(i));
            for(int j=0;j < rowPutChange.getColumnsToPut().size(); j++){
                assertEquals(rowPutChange.getColumnsToPut().get(j).getName(),reqRow.getColumns()[j].getName());
                assertEquals(rowPutChange.getColumnsToPut().get(j).getValue().asDateTime(),reqRow.getColumns()[j].getValue().asDateTime());
            }
        }
    }

    @Test
    public void testAddDefinedColumnWithDateTime() throws Exception {

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk1",PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pkdatetime",PrimaryKeyType.DATETIME);

        tableMeta.addDefinedColumn("coldatetime",DefinedColumnType.DATETIME);
        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxVersions(3);
        CreateTableRequest createTableRequest = new CreateTableRequest(tableMeta,tableOptions);

        client.createTable(createTableRequest);
        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);
        DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);

        assertEquals(describeTableResponse.getTableOptions().getTimeToLive(),tableOptions.getTimeToLive());
        assertEquals(describeTableResponse.getTableOptions().getMaxVersions(),tableOptions.getMaxVersions());
        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().size(),1);
        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().get(0),tableMeta.getDefinedColumnsList().get(0));

        //add defined columns
        AddDefinedColumnRequest addDefinedColumnRequest = new AddDefinedColumnRequest();
        addDefinedColumnRequest.setTableName(tableName);
        addDefinedColumnRequest.addDefinedColumn("coldatetime1",DefinedColumnType.DATETIME);
        client.addDefinedColumn(addDefinedColumnRequest);

        describeTableResponse = client.describeTable(describeTableRequest);

        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().size(),2);
        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().get(0),tableMeta.getDefinedColumnsList().get(0));
        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().get(1),addDefinedColumnRequest.getDefinedColumn().get(0));

        //delete defined columns
        DeleteDefinedColumnRequest deleteDefinedColumnRequest = new DeleteDefinedColumnRequest();
        deleteDefinedColumnRequest.setTableName(tableName);
        deleteDefinedColumnRequest.addDefinedColumn("coldatetime");
        deleteDefinedColumnRequest.addDefinedColumn("coldatetime1");

        client.deleteDefinedColumn(deleteDefinedColumnRequest);
        describeTableResponse = client.describeTable(describeTableRequest);
        assertEquals(describeTableResponse.getTableMeta().getDefinedColumnsList().size(),0);
    }


}
