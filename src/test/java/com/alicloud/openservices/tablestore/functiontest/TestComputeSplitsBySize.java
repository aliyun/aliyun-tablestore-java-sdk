package com.alicloud.openservices.tablestore.functiontest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.junit.*;

import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeResponse;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.Split;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;

import static org.junit.Assert.*;

public class TestComputeSplitsBySize {

    private long sleepTime = 5l * 1000l;

    static SyncClient internalClient = null;
    static SyncClient publicClient = null;

    @Before
    public  void before() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String publicInstanceName = settings.getOTSInstanceName();
        final String internalInstanceName =  settings.getOTSInstanceNameInternal();
        internalClient = new SyncClient(endPoint, accessId, accessKey, internalInstanceName);
        publicClient = new SyncClient(endPoint, accessId, accessKey, publicInstanceName);
    }
    @After
    public void after() {
        internalClient.shutdown();
        publicClient.shutdown();
    }

    /**
     * 测试目的：1) 验证ComputeSplitsBySize功能可以正常地将表格中的数据划分为多个数据块。
     *           2) 验证ComputeSplitsBySize功能SplitLimit参数是否可以正确限制Split返回个数。
     * 测试内容：1）调用ComputeSplitsBySize接口获得该表格的数据分块，并检查所返回的数据中分块数目大于1。
     *           2）不限制SplitPointLimit获取Split个数，然后设置SplitPointLimit后再次获取Split，并检测返回Split个数是否符合预期：
     *              A、 0 << SplitPointLimit = 9 << Split.size();
     *              B、 SplitPointLimit = 1;
     *              C、 SplitPointLimit = Split.size() - 1;
     *              D、 SplitPointLimit = Split.size() ;
     *              E、 SplitPointLimit = Split.size() - 2;
     *              F、 SplitPointLimit = 0, -1。
     * @throws InterruptedException
     */
    @Test
    public void testComputeSplitsBySize() throws InterruptedException {

        // Testing configuration
        String tableName = "testtargettable";
        String primaryKeyPrefix = "primarykey_";
        String columnNamePrefix = "columnName_";
        int maxNumberPrimaryKeysEachRow = 4;
        int maxNumberColumnsEachRow = 128;
        int defaultCuRead = 0;
        int defaultCuWrite = 0;

        SyncClient client = publicClient;
        WriterConfig config = new WriterConfig();

        // Creating testing data
        boolean ifCreateTable = true;
        ListTableResponse ltr = client.listTable();
        for (String tn : ltr.getTableNames()) {
            if (tn.equals(tableName)) {
                ifCreateTable = false;
            }
        }
        if (ifCreateTable) {
            TableMeta tm = new TableMeta(tableName);
            for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                tm.addPrimaryKeyColumn(primaryKeyPrefix + j, PrimaryKeyType.STRING);
            }
            TableOptions to = new TableOptions(86400, 5, 86400);
            ReservedThroughput rtp = new ReservedThroughput(new CapacityUnit(defaultCuRead, defaultCuWrite));
            CreateTableRequest ctr = new CreateTableRequest(tm, to, rtp);
            client.createTable(ctr);
            Thread.sleep(sleepTime);

            TableStoreWriter writer = new DefaultTableStoreWriter(client.asAsyncClient(), tableName, config, null, Executors.newFixedThreadPool(3));
            long startTime = System.currentTimeMillis();
            int bn = 0;
            for (int i = 0; i < 100000; i++) {
                RowPutChange brpc = new RowPutChange(tableName);
                List<PrimaryKeyColumn> pkcl = new ArrayList<PrimaryKeyColumn>();
                for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                    String ss = primaryKeyPrefix + j;
                    pkcl.add(new PrimaryKeyColumn(ss, PrimaryKeyValue.fromString(ss + "_" + i + "_" + bn)));
                }
                PrimaryKey bwpk = new PrimaryKey(pkcl);
                brpc.setPrimaryKey(bwpk);
                for (int k = 0; k < maxNumberColumnsEachRow; ++k) {
                    String ss = columnNamePrefix + k;
                    brpc.addColumn(ss, ColumnValue.fromString(ss));
                }
                writer.addRowChange(brpc);
            }

            writer.flush();
        }

        /**
         * 1) 验证ComputeSplitsBySize功能可以正常地将表格中的数据划分为多个数据块。
         */
        long splitSize = 1;
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(tableName);
        csbsr.setSplitSizeInByte(1, 1024 * 1024);
        ComputeSplitsBySizeResponse csbsrr = null;
        try {
            csbsrr = client.computeSplitsBySize(csbsr);
        } catch( ClientException ce ) {
            ce.printStackTrace();
            fail();
        }

        // To check and print the response.
        assertTrue(csbsrr.getSplits().size() > 0);
        for (Split split : csbsrr.getSplits()) {
            assertNotNull(split.getLocation());
            assertTrue(!split.getLocation().equals(""));

            System.out.println(split.getLowerBound().toString());
            System.out.println(split.getUpperBound().toString());
            System.out.println(split.getLocation());
        }

        GetRangeRequest grr = new GetRangeRequest();
        RangeRowQueryCriteria rrqc = new RangeRowQueryCriteria(tableName);
        rrqc.setDirection(Direction.FORWARD);
        rrqc.setInclusiveStartPrimaryKey(csbsrr.getSplits().get(0).getLowerBound());
        PrimaryKey endPK = csbsrr.getSplits().get(0).getUpperBound();
        if (endPK.getPrimaryKeyColumns().length <= 1) {
            List<PrimaryKeyColumn> pkcl = new ArrayList<PrimaryKeyColumn>();
            pkcl.add(endPK.getPrimaryKeyColumn(0));
            for (int i = 1; i < maxNumberPrimaryKeysEachRow; ++i) {
                String ss = primaryKeyPrefix + i;
                pkcl.add(new PrimaryKeyColumn(ss, PrimaryKeyValue.INF_MIN));
            }
            endPK = new PrimaryKey(pkcl);
        }
        rrqc.setExclusiveEndPrimaryKey(endPK);
        rrqc.setMaxVersions(5);
        grr.setRangeRowQueryCriteria(rrqc);
        GetRangeResponse grrs = null;
        try {
            grrs = client.getRange(grr);
        } catch ( ClientException ce ) {
            ce.printStackTrace();
            fail();
        }
        assertTrue(String.format("%d", grrs.getRows().size()), grrs.getRows().size() > 0);

        /**
         * 2) 验证ComputeSplitsBySize功能SplitLimit参数是否可以正确限制Split返回个数。
         */
        {
            // A、 0 << SplitPointLimit = 9 << Split.size();
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);
            csbsrLimit.setSplitPointLimit(2);
            ComputeSplitsBySizeResponse csbsrrLimit = null;
            try {
                csbsrrLimit = client.computeSplitsBySize(csbsrLimit);
            } catch (ClientException ce) {
                ce.printStackTrace();
                fail();
            }

            // To check and print the response.

            assertEquals(3, csbsrrLimit.getSplits().size());
            for (Split split : csbsrr.getSplits()) {
                assertNotNull(split.getLocation());
                assertTrue(!split.getLocation().equals(""));
            }
        }

        {
            // B、 SplitPointLimit = 1;
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);
            csbsrLimit.setSplitPointLimit(1);
            ComputeSplitsBySizeResponse csbsrrLimit = null;
            try {
                csbsrrLimit = client.computeSplitsBySize(csbsrLimit);
            } catch (ClientException ce) {
                ce.printStackTrace();
                fail();
            }

            // To check and print the response.
            assertEquals(2, csbsrrLimit.getSplits().size());
            for (Split split : csbsrr.getSplits()) {
                assertNotNull(split.getLocation());
                assertTrue(!split.getLocation().equals(""));
            }
        }

        {
            // C、 SplitPointLimit = Split.size() - 1;
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);
            csbsrLimit.setSplitPointLimit(csbsrr.getSplits().size() - 1);
            ComputeSplitsBySizeResponse csbsrrLimit = null;
            try {
                csbsrrLimit = client.computeSplitsBySize(csbsrLimit);
            } catch (ClientException ce) {
                ce.printStackTrace();
                fail();
            }

            // To check and print the response.
            assertEquals(csbsrr.getSplits().size(), csbsrrLimit.getSplits().size());
            for (Split split : csbsrr.getSplits()) {
                assertNotNull(split.getLocation());
                assertTrue(!split.getLocation().equals(""));
            }
        }

        {
            // D、 SplitPointLimit = Split.size();
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);
            csbsrLimit.setSplitPointLimit(csbsrr.getSplits().size());
            ComputeSplitsBySizeResponse csbsrrLimit = null;
            try {
                csbsrrLimit = client.computeSplitsBySize(csbsrLimit);
            } catch (ClientException ce) {
                ce.printStackTrace();
                fail();
            }

            // To check and print the response.
            assertEquals(csbsrr.getSplits().size(), csbsrrLimit.getSplits().size());
            for (Split split : csbsrr.getSplits()) {
                assertNotNull(split.getLocation());
                assertTrue(!split.getLocation().equals(""));
            }
        }

        {
            // E、 SplitPointLimit = Split.size() - 2;
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);
            csbsrLimit.setSplitPointLimit(csbsrr.getSplits().size() - 2);
            ComputeSplitsBySizeResponse csbsrrLimit = null;
            try {
                csbsrrLimit = client.computeSplitsBySize(csbsrLimit);
            } catch (ClientException ce) {
                ce.printStackTrace();
                fail();
            }

            // To check and print the response.
            assertEquals(csbsrr.getSplits().size() - 1, csbsrrLimit.getSplits().size());
            for (Split split : csbsrr.getSplits()) {
                assertNotNull(split.getLocation());
                assertTrue(!split.getLocation().equals(""));
            }
        }

        {
            //F、 SplitPointLimit = 0, -1。
            ComputeSplitsBySizeRequest csbsrLimit = new ComputeSplitsBySizeRequest();
            csbsrLimit.setTableName(tableName);
            csbsrLimit.setSplitSizeInByte(1, 1024 * 1024);

            // SplitPointLimit = 0
            try {
                csbsrLimit.setSplitPointLimit(0);
                fail();
            } catch (IllegalArgumentException e){
                assertEquals("The value of SplitPointLimit must be greater than 0.", e.getMessage());
            }

            // SplitPointLimit = -1
            try {
                csbsrLimit.setSplitPointLimit(-1);
                fail();
            } catch (IllegalArgumentException e){
                assertEquals("The value of SplitPointLimit must be greater than 0.", e.getMessage());
            }

        }


        client.shutdown();
    }

    /**
     * 测试目的：验证ComputeSplitsBySize功能可以正常地在空表格中进行操作。
     * 测试内容：调用ComputeSplitsBySize接口获得该表格的数据分块，并检查所返回的数据中分块数目等于1因为表格中没有数据。
     * @throws InterruptedException
     */
    @Test
    public void testComputeSplitsBySizeRequestWithEmptyDataSet() throws InterruptedException {

        // Testing configuration
        String tableName = "testtargettable2";
        String primaryKeyPrefix = "primarykey_";
        int maxNumberPrimaryKeysEachRow = 4;
        int defaultCuRead = 0;
        int defaultCuWrite = 0;
        SyncClient client = publicClient;

        // Creating empty table
        ListTableResponse ltr = client.listTable();
        for (String tn : ltr.getTableNames()) {
            if (tn.equals(tableName)) {
                DeleteTableRequest dtr = new DeleteTableRequest(tableName);
                client.deleteTable(dtr);
            }
        }
        {
            TableMeta tm = new TableMeta(tableName);
            for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                tm.addPrimaryKeyColumn(primaryKeyPrefix + j, PrimaryKeyType.STRING);
            }
            TableOptions to = new TableOptions(86400, 5, 86400);
            ReservedThroughput rtp = new ReservedThroughput(new CapacityUnit(defaultCuRead, defaultCuWrite));
            CreateTableRequest ctr = new CreateTableRequest(tm, to, rtp);
            client.createTable(ctr);
            Thread.sleep(sleepTime);
        }

        // ComputeSplitsBySize operation
        long splitSize = 1l;
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(tableName);
        csbsr.setSplitSizeIn100MB(splitSize);
        ComputeSplitsBySizeResponse csbsrr = null;
        try {
            csbsrr = client.computeSplitsBySize(csbsr);
        } catch( TableStoreException tse ) {
            tse.printStackTrace();
            fail();
        } catch( ClientException ce ) {
            ce.printStackTrace();
            fail();
        }

        // To check and print the response.
        assertTrue(csbsrr.getSplits().size() == 1);
        for (Split split : csbsrr.getSplits()) {
            assertNotNull(split.getLocation());
            assertTrue(!split.getLocation().equals(""));
        }

        client.shutdown();
    }

    /**
     * 测试目的：验证ComputeSplitsBySize功能在访问不存在的表格的时候会报出异常。
     * 测试内容：调用ComputeSplitsBySize接口在访问不存在的表格的时候会报出异常，且异常信息中会包含表格存在的错误异常信息。
     * @throws InterruptedException
     */
    @Test
    public void testComputeSplitsBySizeRequestWithNotExistedTable() throws InterruptedException {

        // Testing configuration
        String tableName = "testtargettable3";
        SyncClient client = publicClient;

        // Creating empty table
        ListTableResponse ltr = client.listTable();
        for (String tn : ltr.getTableNames()) {
            if (tn.equals(tableName)) {
                DeleteTableRequest dtr = new DeleteTableRequest(tableName);
                client.deleteTable(dtr);
                Thread.sleep(sleepTime);
            }
        }

        long splitSize = 1l;
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(tableName);
        csbsr.setSplitSizeIn100MB(splitSize);
        try {
            client.computeSplitsBySize(csbsr);
            fail();
        } catch( TableStoreException tse ) {
            if ( !tse.getMessage().contains("Requested table does not exist.") ) {
                fail();
            }
        }
    }

    @Test
    public void testComputeSplitsBySizeRequestWithNegativeSplitSize() throws InterruptedException {
        // Testing configuration
        String tableName = "testtargettable3";
        String primaryKeyPrefix = "primarykey_";
        int maxNumberPrimaryKeysEachRow = 4;
        int defaultCuRead = 0;
        int defaultCuWrite = 0;
        SyncClient client = publicClient;

        // Creating empty table
        ListTableResponse ltr = client.listTable();
        for (String tn : ltr.getTableNames()) {
            if (tn.equals(tableName)) {
                DeleteTableRequest dtr = new DeleteTableRequest(tableName);
                client.deleteTable(dtr);
                Thread.sleep(sleepTime);
            }
        }
        {
            TableMeta tm = new TableMeta(tableName);
            for (int j = 0; j < maxNumberPrimaryKeysEachRow; ++j) {
                tm.addPrimaryKeyColumn(primaryKeyPrefix + j, PrimaryKeyType.STRING);
            }
            TableOptions to = new TableOptions(86400, 5, 86400);
            ReservedThroughput rtp = new ReservedThroughput(new CapacityUnit(defaultCuRead, defaultCuWrite));
            CreateTableRequest ctr = new CreateTableRequest(tm, to, rtp);
            client.createTable(ctr);
            Thread.sleep(sleepTime);
        }

        long splitSize = -1l;
        ComputeSplitsBySizeRequest csbsr = new ComputeSplitsBySizeRequest();
        csbsr.setTableName(tableName);
        csbsr.setSplitSizeIn100MB(splitSize);
        try {
            client.computeSplitsBySize(csbsr);
            fail();
        } catch( TableStoreException tse ) {
            assertEquals(ErrorCode.INVALID_PARAMETER, tse.getErrorCode());
            assertEquals("The split_size should be greater than 0.", tse.getMessage());
        }
    }

}
