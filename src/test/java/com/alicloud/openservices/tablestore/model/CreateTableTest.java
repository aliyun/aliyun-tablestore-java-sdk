package com.alicloud.openservices.tablestore.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.google.gson.JsonSyntaxException;

public class CreateTableTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "CreateTableTest";
    private static SyncClientInterface client;
    private static Logger LOG = Logger.getLogger(BatchWriteTest.class.getName());
    
    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        client = Utils.getOTSInstance();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(client);
    }
    
    public static boolean checkNameExiste(List<String> names, String name) {
        for (String n : names) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }
    
    public void testCommon(SyncClientInterface ots, PrimaryKeyType type,
                           int writeCU, int readCU, int timeToLive, int maxVersions) throws Exception {
    	int pkNum = 1 + ((int)Math.random() % 4);
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
    	for (int i = 0; i < pkNum; ++i) {
    		pks.put("PK" + Integer.toString(i), type);
    	}
        
        OTSHelper.createTable(ots, tableName, pks, readCU, writeCU, timeToLive, maxVersions);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        
        DescribeTableResponse result = OTSHelper.describeTable(ots, tableName);
        
        assertEquals(pkNum, result.getTableMeta().getPrimaryKeyList().size());
        
        for (int i = 0; i < pkNum; ++i) {
        	assertEquals("PK" + Integer.toString(i), result.getTableMeta().getPrimaryKeyList().get(i).getName());
            assertEquals(type, result.getTableMeta().getPrimaryKeyList().get(i).getType());
        }
        
        assertEquals(readCU, result.getReservedThroughputDetails().getCapacityUnit().getReadCapacityUnit());
        assertEquals(writeCU, result.getReservedThroughputDetails().getCapacityUnit().getWriteCapacityUnit());
        
        assertTrue(result.getTableOptions().hasSetMaxVersions());
        assertTrue(result.getTableOptions().hasSetTimeToLive());
        
        assertEquals(maxVersions, result.getTableOptions().getMaxVersions());
        assertEquals(timeToLive, result.getTableOptions().getTimeToLive());
    }

    /**
     * 创建表的时候，TableMeta中有2个PK列，列名重复，期望返回OTSParameterInvalid，list table()确认没有这个表
     * @throws Exception
     */
    @Test
    public void testWithDuplicatePK() throws Exception {
        try {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.STRING);
            CreateTableRequest request = new CreateTableRequest(tableMeta, new com.alicloud.openservices.tablestore.model.TableOptions(-1, 1));
            request.setReservedThroughput(new ReservedThroughput(5000, 4000));
            client.createTable(request);
            assertTrue(false);
        } catch (TableStoreException e) {
            assertEquals(ErrorCode.INVALID_PARAMETER, e.getErrorCode());
        }
        
        assertFalse(checkNameExiste(OTSHelper.listTable(client), tableName));
    }
    
    /**
     * 建表的时候，0 CU，期望正常，describe table()获取信息与创表参数一致
     * @throws Exception
     */
    @Test
    public void testWithZeroCU() throws Exception {
        int writeCU = 0;
        int readCU = 0;
        int timeToLive = 86400;
        int maxVersions = (int)(Math.random() * 100) + 1;
        PrimaryKeyType type = PrimaryKeyType.STRING;
        testCommon(client, type, writeCU, readCU, timeToLive, maxVersions);
    }
    
    /**
     * 建表的时候，timeToLive为-1，期望正常，describe table()获取信息与创表参数一致
     * @throws Exception
     */
    @Test
    public void testWithNoTTL() throws Exception {
        int writeCU = 0;
        int readCU = 0;
        int timeToLive = -1;
        int maxVersions = (int)(Math.random() * 100) + 1;
        PrimaryKeyType type = PrimaryKeyType.STRING;
        testCommon(client, type, writeCU, readCU, timeToLive, maxVersions);
    }
    
    /**
     * 建表的时候，都为STRING类型，期望正常，describe table()获取信息与创表参数一致
     * @throws Exception
     */
    @Test
    public void testWithStringPK() throws Exception {
        int writeCU = 0;
        int readCU = 0;
        int timeToLive = 86400;
        int maxVersions = (int)(Math.random() * 100) + 1;
        PrimaryKeyType type = PrimaryKeyType.STRING;
        testCommon(client, type, writeCU, readCU, timeToLive, maxVersions);
    }
    
    /**
     * 创建表的时候，TableMeta中有4个PK列，都为INTEGER类型，期望正常，describe table()获取信息与创表参数一致
     * @throws Exception
     */
    @Test
    public void testWithIntegerPK() throws Exception {
        int writeCU = 0;
        int readCU = 0;
        int timeToLive = 86400;
        int maxVersions = (int)(Math.random() * 100) + 1;
        PrimaryKeyType type = PrimaryKeyType.INTEGER;
        testCommon(client, type, writeCU, readCU, timeToLive, maxVersions);
    }
    
    /**
     * 创建表的时候，TableMeta中有4个PK列，都为BINARY类型，期望正常，describe table()获取信息与创表参数一致
     * @throws Exception
     */
    @Test
    public void testWithBinaryPK() throws Exception {
        int writeCU = 0;
        int readCU = 0;
        int timeToLive = 86400;
        int maxVersions = (int)(Math.random() * 100) + 1;
        PrimaryKeyType type = PrimaryKeyType.BINARY;
        testCommon(client, type, writeCU, readCU, timeToLive, maxVersions);
    }
    
    /**
     * public实例，建表指定maxTimeDeviation
     * @throws Exception
     */
    @Test
    public void testSetMaxTimeDeviation() throws Exception {
    	Map<String, PrimaryKeyType> pks = new TreeMap<String, PrimaryKeyType>();
    	pks.put("PK0", PrimaryKeyType.STRING);

    	TableOptions tableOptions = new TableOptions(-1, 1);
    	int deviation = 256 * 1024;
    	tableOptions.setMaxTimeDeviation(deviation);
        CreateTableRequest request = new CreateTableRequest(OTSHelper.getTableMeta(tableName, pks), tableOptions);
        request.setReservedThroughput(new ReservedThroughput(0, 0));
        client.createTable(request);
        Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);
        
        DescribeTableResponse result = OTSHelper.describeTable(client, tableName);
        assertEquals(result.getTableOptions().getMaxTimeDeviation(), deviation);
    }
}
