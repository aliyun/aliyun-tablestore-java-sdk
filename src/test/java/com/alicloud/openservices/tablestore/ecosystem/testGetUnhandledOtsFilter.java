package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class testGetUnhandledOtsFilter {


    private static SyncClient client;
    public static ComputeParameters para;
    private static final Gson GSON = new Gson();

    public static String endpoint = "http://iquandata.ali-cn-hangzhou.ots.aliyuncs.com";
    public static String accessKey = "";
    public static String secret = "";
    public static String instanceName = "iquandata";
    public static String tableName = "wenxian_searchIndex_spark_test";
    public static String indexName = "wenxian_searchIndex_spark_test_index2";

    public static Filter filterPk1StartWith = new Filter(Filter.CompareOperator.START_WITH, "pk1", ColumnValue.fromString("0000pppeee4444"));
    public static Filter filterPk1Equal = new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromString("0000pppeee4444tttiii8888xxxmmm"));
    public static Filter filterPk1NotEqual = new Filter(Filter.CompareOperator.NOT_EQUAL, "pk1", ColumnValue.fromString("0000pppeee4444tttiii8888xxxmmm"));
    public static Filter filterIsNull = new Filter(Filter.CompareOperator.IS_NULL, "val_long1");
    public static Filter filterLessThan = new Filter(Filter.CompareOperator.LESS_THAN, "val_long1", ColumnValue.fromLong(98350700));
    public static Filter filterGreaterThan = new Filter(Filter.CompareOperator.GREATER_THAN, "val_long1", ColumnValue.fromLong(3697900));
    public static Filter filterLessTanOrEqual = new Filter(Filter.CompareOperator.LESS_EQUAL, "val_long1", ColumnValue.fromLong(98350700));
    public static Filter filterGreaterThanOrEqual = new Filter(Filter.CompareOperator.GREATER_EQUAL, "val_long1", ColumnValue.fromLong(3697900));
    public static Filter filterGeoDistance = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"centerPoint\":\"6,9\", \"distanceInMeter\": 100000}"));
    public static Filter filterGeoBoundingBox = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"topLeft\":\"3,0\", \"bottomRight\": \"0,3\"}"));
    public static Filter filterGeoPolygon = new Filter(Filter.CompareOperator.EQUAL, "val_geo", ColumnValue.fromString("{\"points\":[\"5,0\",\"5,1\", \"6,1\", \"6,10\"]}"));


    public static ArrayList<String> requiredColumns = new ArrayList<String>();

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        resetOrGeoFlag();
        ClientConfiguration conf = new ClientConfiguration();
        client = new SyncClient(
                endpoint,
                accessKey,
                secret,
                instanceName,
                conf);
        requiredColumns.add("pk1");
        requiredColumns.add("val_keyword1");
        requiredColumns.add("val_keyword2");
        requiredColumns.add("val_keyword3");
        requiredColumns.add("val_bool");
        requiredColumns.add("val_double");
        requiredColumns.add("val_long1");
        requiredColumns.add("val_long2");
        requiredColumns.add("val_text");
        requiredColumns.add("val_geo");

        para = new ComputeParameters(indexName, 16);
    }

    @AfterClass
    public static void classAfter() {
        client.shutdown();
    }

    private static void resetOrGeoFlag() {
        TablestoreSplit.containOr = false;
        TablestoreSplit.containGeo = false;
    }

    @Test
    //  a < 1 and b like 'begin%'     =>    a<1
    public void test1() {
        ArrayList<Filter> filters1 = new ArrayList<Filter>();
        filters1.add(filterLessThan);
        filters1.add(filterPk1StartWith);
        Filter filterOld = new Filter(Filter.LogicOperator.AND, filters1);


        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filterOld, tableName, para, requiredColumns);
        for (ITablestoreSplit split : splits) {
            Filter unhandledOtsFilter = ((TablestoreSplit) split).getUnhandledOtsFilter(client, filterOld, tableName, indexName, new FilterPushdownConfig(false, false));
            Assert.assertEquals(filterLessThan, unhandledOtsFilter);
        }
    }

    @Test

    public void test4() {
        ArrayList<Filter> filters1 = new ArrayList<Filter>();
        filters1.add(filterPk1Equal);
        filters1.add(filterPk1StartWith);
        filters1.add(filterGeoDistance);
        Filter filterOld = new Filter(Filter.LogicOperator.OR, filters1);

        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filterOld, tableName, para, requiredColumns);
        for (ITablestoreSplit split : splits) {
            Filter unhandledOtsFilter = ((TablestoreSplit) split).getUnhandledOtsFilter(client, filterOld, tableName, indexName, new FilterPushdownConfig(false, false));
            Assert.assertEquals(null, unhandledOtsFilter);
        }
    }

    @Test
    // (a = 1 and geodistance )or ( geodistance1 and geodistance2)  => a = 1
    public void test2() {
        List<Filter> filters1 = new ArrayList<Filter>();
        filters1.add(filterPk1Equal);
        filters1.add(filterGeoDistance);
        List<Filter> filters2 = new ArrayList<Filter>();
        filters2.add(filterGeoBoundingBox);
        filters2.add(filterGeoPolygon);
        ArrayList<Filter> filters3 = new ArrayList<Filter>();
        filters3.add(new Filter(Filter.LogicOperator.AND, filters1));
        filters3.add(new Filter(Filter.LogicOperator.AND, filters2));
        Filter filterOld = new Filter(Filter.LogicOperator.OR, filters3);

        Filter filterExpected = null;

        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filterOld, tableName, para, requiredColumns);
        for (ITablestoreSplit split : splits) {
            Filter unhandledOtsFilter = ((TablestoreSplit) split).getUnhandledOtsFilter(client, filterOld, tableName, indexName, new FilterPushdownConfig(false, false));
            Assert.assertEquals(filterExpected, unhandledOtsFilter);
        }

    }

    @Test
    // (a > 1 and geodistance )or ( geodistance1 and geodistance2)  => error
    public void test3() {
        List<Filter> filters1 = new ArrayList<Filter>();
        filters1.add(filterGreaterThan);
        filters1.add(filterGeoDistance);
        List<Filter> filters2 = new ArrayList<Filter>();
        filters2.add(filterGeoBoundingBox);
        filters2.add(filterGeoPolygon);
        ArrayList<Filter> filters3 = new ArrayList<Filter>();
        filters3.add(new Filter(Filter.LogicOperator.AND, filters1));
        filters3.add(new Filter(Filter.LogicOperator.AND, filters2));
        Filter filterOld = new Filter(Filter.LogicOperator.OR, filters3);

        Filter filterExpected = null;

        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(client);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(client, filterOld, tableName, para, requiredColumns);
        for (ITablestoreSplit split : splits) {
            Filter unhandledOtsFilter = ((TablestoreSplit) split).getUnhandledOtsFilter(client, filterOld, tableName, indexName, new FilterPushdownConfig(true, true));
            Assert.assertEquals(filterExpected, unhandledOtsFilter);
        }
    }
}