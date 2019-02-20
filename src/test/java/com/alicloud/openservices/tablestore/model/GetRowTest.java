package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.google.gson.JsonSyntaxException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class GetRowTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "GetRowTest";
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

    /**
     * 不指定列读，public返回的列中不包含主键列
     * @throws Exception
     */
    @Test
    public void testReadWithoutColumnsForPublic() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

    	CreateTable(client, tableName, pks);

        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .build();

        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns);

        GetRowResponse response = OTSHelper.getRow(client, tableName, pk);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 1);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
    }

    /**
     * 指定主键列读，public返回的列中不包含主键列
     * @throws Exception
     */
    @Test
    public void testReadPKColumnsForPublic() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);

    	CreateTable(client, tableName, pks);

        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .build();

        Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
        columns.put("attr", ColumnValue.fromString("hello world"));
        OTSHelper.putRow(client, tableName, pk, columns);

        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.addColumnsToGet("PK1");
        query.addColumnsToGet("attr");
        query.setMaxVersions(1);
        GetRowResponse response = OTSHelper.getRow(client, query);
        Row row = response.getRow();
        assertEquals(row.getColumns().length, 1);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
    }
}
