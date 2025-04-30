package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.SearchIndexInfo;
import com.google.gson.JsonSyntaxException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BatchWriteTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "BatchWriteTest";
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
        OTSHelper.deleteAllTable(client);
    }

    private void CreateTable(SyncClientInterface ots, String tableName, Map<String, PrimaryKeyType> pk) throws Exception {
        OTSHelper.createTable(ots, tableName, pk);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
    }
    
    /**
     * Public write duplicate row
     * @throws Exception
     */
    @Test
    public void testBatchWriteDuplicateRowsForPublic() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
        pks.put("PK1", PrimaryKeyType.STRING);
        
    	CreateTable(client, tableName, pks);
    	
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK1", PrimaryKeyValue.fromString("haha"))
                .build();
        
        List<RowUpdateChange> updates = new ArrayList<RowUpdateChange>();
        RowUpdateChange updateChange = new RowUpdateChange(tableName, pk);
        updateChange.put("attr", ColumnValue.fromString("hello world"));
        updates.add(updateChange);
        updates.add(updateChange);
        BatchWriteRowResponse writeResponse = OTSHelper.batchWriteRow(client, null, updates, null);
        assertTrue(writeResponse.isAllSucceed());
        
        SingleRowQueryCriteria query = new SingleRowQueryCriteria(tableName, pk);
        query.addColumnsToGet("PK1");
        query.addColumnsToGet("attr");
        query.setMaxVersions(1);
        GetRowResponse readResponse = OTSHelper.getRow(client, query);
        Row row = readResponse.getRow();
        assertEquals(row.getColumns().length, 1);
        assertEquals(row.getColumn("attr").get(0).getValue().asString(), "hello world");
    }
}
