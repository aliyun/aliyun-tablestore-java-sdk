package com.alicloud.openservices.tablestore.functiontest;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.UpdatePhyTable;
import com.alicloud.openservices.tablestore.model.UpdateGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.UpdateGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.BaseTable;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.GlobalTableStatus;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.PhyTable;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Placement;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Removal;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.SyncMode;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableResponse;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class GlobalTableTest {
    private static String baseInstName = "";
    private static String baseRegion = "";
    private static String testGlobalTableName;
    private static String testGlobalTableId;
    private static SyncClient baseClient = null;
    private static final Map<String, SyncClient> placementClients = Maps.newHashMap();

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();

        baseInstName = settings.getOTSInstanceName();
        baseRegion = "ali-test";
        baseClient = new SyncClient(endPoint, accessId, accessKey, baseInstName);
        testGlobalTableName = "v0_1_table" + "_" + System.currentTimeMillis();

        CreateTableResponse resp = baseClient.createTable(buildSampleCreateTableRequest(testGlobalTableName));
        assertNotNull(resp);
        assertNotNull(resp.getRequestId());
        //for (String placementInstName : placementInstNames) {
        //    placementClients.put(placementInstName, new SyncClient(endPoint, accessId, accessKey, placementInstName));
        //}
    }

    private static CreateTableRequest buildSampleCreateTableRequest(String tableName) {
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addDefinedColumn("col1", DefinedColumnType.INTEGER);
        meta.addDefinedColumn("col2", DefinedColumnType.STRING);

        TableOptions opts = new TableOptions(-1, 1);
        opts.setMaxTimeDeviation(Integer.MAX_VALUE);
        opts.setUpdateFullRow(true); // same things as 'is row version table'

        return new CreateTableRequest(meta, opts);
    }

    @AfterClass
    public static void afterClass() {
        // unbind all table from global table
        if (!Strings.isNullOrEmpty(testGlobalTableId)) {
            UnbindGlobalTableRequest unbReq = new UnbindGlobalTableRequest(testGlobalTableId, testGlobalTableName);

            ArrayList<Removal> removals = Lists.newArrayList();
            //for (String placementInstName : placementInstNames) {
            //    Removal r = new Removal(testRegionId, placementInstName);
            //    removals.add(r);
            //}
            removals.add(new Removal(baseRegion, baseInstName));

            unbReq.setRemovals(removals);
            UnbindGlobalTableResponse unbResp = baseClient.unbindGlobalTable(unbReq);
            assertNotNull(unbResp);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignore) {
        }

        // delete placement table
        //for (String placementInstName : placementInstNames) {
        //    SyncClient placeInstClient = placementClients.getOrDefault(placementInstName, null);
        //    if (placeInstClient!=null) {
        //        placeInstClient.deleteTable(new DeleteTableRequest(testGlobalTableName));
        //        placeInstClient.shutdown();
        //
        //    }
        //}
        // delete base table
        if (!Strings.isNullOrEmpty(testGlobalTableName)) {
            baseClient.deleteTable(new DeleteTableRequest(testGlobalTableName));
            baseClient.shutdown();
        }
    }

    @Test
    @Ignore
    public void testCreateGlobalTable() {
        CreateGlobalTableRequest req = new CreateGlobalTableRequest(
                new BaseTable(baseRegion, baseInstName, testGlobalTableName),
                SyncMode.ROW
        );
        //for (String placementInstName : placementInstNames) {
        //    req.addPlacement(new Placement(testRegionId, placementInstName, false));
        //}
        CreateGlobalTableResponse createResp = baseClient.createGlobalTable(req);
        assertNotNull(createResp);
        assertNotNull(createResp.getGlobalTableId());
        assertNotNull(createResp.getStatus());
        assertEquals(createResp.getStatus(), GlobalTableStatus.RE_CONF);

        // global table id for one test round
        testGlobalTableId = createResp.getGlobalTableId();
    }

    @Test
    @Ignore
    public void testDescribeGlobalTable() {
        DescribeGlobalTableRequest req = new DescribeGlobalTableRequest(testGlobalTableId, testGlobalTableName);
        DescribeGlobalTableResponse descResp = baseClient.describeGlobalTable(req);
        assertNotNull(descResp);
        assertNotNull(descResp.getGlobalTableId());
        assertNotNull(descResp.getStatus());
    }

    @Test
    @Ignore
    public void testDescribeGlobalTableByTableNameArgs() {
        DescribeGlobalTableRequest req = new DescribeGlobalTableRequest(testGlobalTableName, baseRegion, baseInstName);
        DescribeGlobalTableResponse descResp = baseClient.describeGlobalTable(req);
        assertNotNull(descResp);
        assertNotNull(descResp.getGlobalTableId());
        assertNotNull(descResp.getStatus());
    }

    @Test
    @Ignore
    public void testDescribeGlobalTableWithFetchRpo() {
        DescribeGlobalTableRequest req = new DescribeGlobalTableRequest(testGlobalTableId, testGlobalTableName);
        req.setReturnRpo(true);
        DescribeGlobalTableResponse descResp = baseClient.describeGlobalTable(req);
        assertNotNull(descResp);
        assertNotNull(descResp.getGlobalTableId());
        assertNotNull(descResp.getStatus());
        for (PhyTable t : descResp.getPhyTables()) {
            if (t.getRpo()!=null) {
                LocalDateTime zonedRpo = t.getRpo().atZone(ZoneId.systemDefault()).toLocalDateTime();
                String rpoTimeStr = zonedRpo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                System.out.println("global table: " + testGlobalTableName + "phy table: " + t.getTableId() + " rpo: " + rpoTimeStr);
                assertNotNull(descResp);
            }
        }

    }

    //@Test
    //public void testBindGlobalTable() {
    //    BindGlobalTableRequest req = new BindGlobalTableRequest(testGlobalTableId, testGlobalTableName);
    //    req.addPlacement(new Placement(baseRegion, "testBindInstName", false));
    //    BindGlobalTableResponse bindResp = baseClient.bindGlobalTable(req);
    //    assertNotNull(bindResp);
    //    assertNotNull(bindResp.getGlobalTableId());
    //    assertNotNull(bindResp.getStatus());
    //    assertEquals(bindResp.getStatus(), GlobalTableStatus.RE_CONF);
    //}

    @Test
    @Ignore
    public void testUnbindGlobalTable() throws Exception {
        UnbindGlobalTableRequest req = new UnbindGlobalTableRequest(testGlobalTableId, testGlobalTableName);

        req.addRemoval(new Removal(baseRegion, baseInstName));
        UnbindGlobalTableResponse unbResp = baseClient.unbindGlobalTable(req);
        assertNotNull(unbResp);
        assertNotNull(unbResp.getGlobalTableId());
        assertNotNull(unbResp.getStatus());
        assertEquals(unbResp.getStatus(), GlobalTableStatus.RE_CONF);
    }

    @Test
    @Ignore
    public void testUpdateGlobalTable() throws Exception {
        UpdatePhyTable changeTable = new UpdatePhyTable(
                baseRegion,
                baseInstName,
                testGlobalTableName);
        changeTable.setWritable(false);
        UpdateGlobalTableRequest req = new UpdateGlobalTableRequest(testGlobalTableId, testGlobalTableName, changeTable);
        UpdateGlobalTableResponse changeResp = baseClient.updateGlobalTable(req);
        assertNotNull(changeResp);
        assertNotNull(changeResp.getRequestId());
    }

    @Test
    @Ignore
    public void testChangePrimarySecondary() throws Exception {
        UpdatePhyTable changeTable = new UpdatePhyTable(
                baseRegion,
                baseInstName,
                testGlobalTableName);
        changeTable.setWritable(true);
        changeTable.setPrimaryEligible(true);
        UpdateGlobalTableRequest req = new UpdateGlobalTableRequest(
                testGlobalTableId,
                testGlobalTableName,
                changeTable
        );
        UpdateGlobalTableResponse changeResp = baseClient.updateGlobalTable(req);
        assertNotNull(changeResp);
        assertNotNull(changeResp.getRequestId());
    }

}
