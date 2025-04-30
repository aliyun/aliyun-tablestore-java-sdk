package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.google.gson.JsonSyntaxException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class testSplitsGeneration {
    private static SyncClient ots;

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        ClientConfiguration conf = new ClientConfiguration();
        ots = new SyncClient("http://tantan-console.ali-cn-hangzhou.ots.aliyuncs.com", "", "", "tantan-console", conf);
    }

    @AfterClass
    public static void classAfter() {
        ots.shutdown();
    }

    @Test
    public void testEmptyFilter() {
        ComputeParameters para = new ComputeParameters();
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, Filter.emptyFilter(), "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testEqualFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter filter = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_A"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNotEqualFilter() {
        ComputeParameters para = new ComputeParameters();

        Filter filter = new Filter(Filter.CompareOperator.NOT_EQUAL, "UserId", ColumnValue.fromString("user_A"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testGreaterThanFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter filter = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testGreaterEqualFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter filter = new Filter(Filter.CompareOperator.GREATER_EQUAL, "UserId", ColumnValue.fromString("user_Z"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testLessThanFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter filter = new Filter(Filter.CompareOperator.LESS_THAN, "UserId", ColumnValue.fromString("user_D"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testStartWithFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter filter = new Filter(Filter.CompareOperator.START_WITH, "UserId", ColumnValue.fromString("user_D"));
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNestedFilter() {
        ComputeParameters para = new ComputeParameters();
        Filter subfilter = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNestedFilter2() {
        ComputeParameters para = new ComputeParameters();
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_THAN, "UserId", ColumnValue.fromString("user_G"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNestedFilter3() {
        ComputeParameters para = new ComputeParameters();
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_THAN, "OrderId", ColumnValue.fromString("X"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNestedFilter4() {
        ComputeParameters para = new ComputeParameters();
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_THAN, "OrderId", ColumnValue.fromString("X"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.OR, subFilters);
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testNestedFilter5() {
        ComputeParameters para = new ComputeParameters();
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_E"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_THAN, "OrderId", ColumnValue.fromString("X"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.NOT, subFilters);
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, null);
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testProjection() {
        ComputeParameters para = new ComputeParameters();

        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_A"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_EQUAL, "UserId", ColumnValue.fromString("user_B"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection1() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "price", ColumnValue.fromLong(100));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_EQUAL, "price", ColumnValue.fromLong(101));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection2() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.GREATER_THAN, "price", ColumnValue.fromLong(200));
        Filter subfilter3 = new Filter(Filter.CompareOperator.LESS_EQUAL, "price", ColumnValue.fromLong(300));

        List<Filter> subFilters = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        subFilters.add(subfilter3);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection3() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.GREATER_THAN, "UserId", ColumnValue.fromString("user_D"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.LESS_EQUAL, "UserId", ColumnValue.fromString("user_E"));
        List<Filter> subFilters = new ArrayList<Filter>();
        subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection4() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.EQUAL, "OrderId", ColumnValue.fromString("b76c973f-41e9-432a-9028-ed61940d2dc3"));
        List<Filter> subFilters = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        subFilters.add(subfilter1);
        subFilters.add(subfilter2);
        Filter filter = new Filter(Filter.LogicOperator.AND, subFilters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection5() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.EQUAL, "OrderId", ColumnValue.fromString("b76c973f-41e9-432a-9028-ed61940d2dc3"));
        Filter subfilter3 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter4 = new Filter(Filter.CompareOperator.EQUAL, "OrderId", ColumnValue.fromString("163ab177-a239-461f-99a8-2d02a7f302f9"));

        List<Filter> filters1 = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        filters1.add(subfilter1);
        filters1.add(subfilter2);

        List<Filter> filters2 = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        filters2.add(subfilter3);
        filters2.add(subfilter4);

        Filter filter1 = new Filter(Filter.LogicOperator.AND, filters1);
        Filter filter2 = new Filter(Filter.LogicOperator.AND, filters2);

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(filter1);
        filters.add(filter2);

        Filter filter = new Filter(Filter.LogicOperator.OR, filters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                if (row != null) {
                    System.out.println("row:" + row.toString());
                }
            } else {
                break;
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection6() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter2 = new Filter(Filter.CompareOperator.EQUAL, "OrderId", ColumnValue.fromString("b76c973f-41e9-432a-9028-ed61940d2dc3"));
        Filter subfilter3 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_B"));
        Filter subfilter4 = new Filter(Filter.CompareOperator.EQUAL, "OrderId", ColumnValue.fromString("notexist"));

        List<Filter> filters1 = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        filters1.add(subfilter1);
        filters1.add(subfilter2);

        List<Filter> filters2 = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        filters2.add(subfilter3);
        filters2.add(subfilter4);

        Filter filter1 = new Filter(Filter.LogicOperator.AND, filters1);
        Filter filter2 = new Filter(Filter.LogicOperator.AND, filters2);

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(filter1);
        filters.add(filter2);

        Filter filter = new Filter(Filter.LogicOperator.OR, filters);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                if (row != null) {
                    System.out.println("row:" + row.toString());
                }
            } else {
                break;
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection7() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "l_orderkey", ColumnValue.fromLong(1));
        Filter subfilter2 = new Filter(Filter.CompareOperator.EQUAL, "l_linenumber", ColumnValue.fromLong(1));

        List<Filter> filters1 = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        filters1.add(subfilter1);
        filters1.add(subfilter2);


        Filter filter1 = new Filter(Filter.LogicOperator.AND, filters1);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter1, "tpch_lineitem_perf", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                if (row != null) {
                    System.out.println("row:" + row.toString());
                }
            } else {
                break;
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection8() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "l_orderkey", ColumnValue.fromLong(20000));
        List<String> columnToGet = new ArrayList<String>();
        //columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, subfilter1, "tpch_lineitem_perf", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                if (row != null) {
                    System.out.println("row:" + row.toString());
                }
            } else {
                break;
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection9() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.EQUAL, "UserId", ColumnValue.fromString("user_E"));

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, subfilter1, "OrderSource", para, columnToGet);
        ITablestoreSplit anySplit = splits.get(0);
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection10() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.LESS_EQUAL, "l_shipdate", ColumnValue.fromString("1993-12-01"));

        List<String> columnToGet = new ArrayList<String>();
        //columnToGet.add("timestamp");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, subfilter1, "tpch_lineitem_perf", para, columnToGet);
        TablestoreSplit anySplit = (TablestoreSplit) splits.get(0);

        TablestoreSplit anySplitX = new TablestoreSplit(anySplit.getType(), subfilter1, columnToGet);
        anySplitX.setSplitName(anySplit.getSplitName());
        anySplitX.setTableName(anySplit.getTableName());
        anySplitX.setKvSplit(anySplit.getKvSplit());
        anySplitX.initial(ots);

        System.out.println("splitinfo:" + anySplitX.toString());

        Iterator<Row> rows = anySplitX.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection11() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);
        Filter subfilter1 = new Filter(Filter.CompareOperator.LESS_EQUAL, "l_shipdate", ColumnValue.fromString("1996-12-01"));

        List<String> columnToGet = new ArrayList<String>();
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, subfilter1, "tpch_lineitem_perf", para, columnToGet);
        TablestoreSplit anySplit = (TablestoreSplit) splits.get(0);

        TablestoreSplit anySplitX = new TablestoreSplit(anySplit.getType(), subfilter1, columnToGet);
        anySplitX.setSplitName(anySplit.getSplitName());
        anySplitX.setTableName(anySplit.getTableName());
        anySplitX.setKvSplit(anySplit.getKvSplit());
        anySplitX.initial(ots);

        System.out.println("splitinfo:" + anySplitX.toString());

        Iterator<Row> rows = anySplitX.getRowIterator(ots);
        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            }
        }
        System.out.println("count:" + splits.size());
    }

    @Test
    public void testIndexSelection12() {
        ComputeParameters para = new ComputeParameters();
        para.setComputeMode(ComputeParameters.ComputeMode.Auto);

        List<String> columnToGet = new ArrayList<String>();
        //columnToGet.add("col1");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);
        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, Filter.emptyFilter(), "spark_test", para, columnToGet);
        TablestoreSplit anySplit = (TablestoreSplit) splits.get(0);
        System.out.println("count:" + splits.size());

        System.out.println("splitinfo:" + anySplit.toString());

        TablestoreSplit anySplitX = new TablestoreSplit(anySplit.getType(), Filter.emptyFilter(), columnToGet);
        anySplitX.setSplitName(anySplit.getSplitName());
        anySplitX.setTableName(anySplit.getTableName());
        anySplitX.setKvSplit(anySplit.getKvSplit());
        anySplitX.initial(ots);

        Iterator<Row> rows = anySplitX.getRowIterator(ots);


        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            } else {
                break;
            }
        }
    }

    @Test
    public void testIndexSelection13() {
        ComputeParameters para = new ComputeParameters("timestream_share_car_order_index", 1);

        List<String> columnToGet = new ArrayList<String>();
        columnToGet.add("order_id");
        columnToGet.add("from");
        ITablestoreSplitManager manager = new DefaultTablestoreSplitManager(ots);

        Filter filter = new Filter(Filter.CompareOperator.EQUAL, "order_id", ColumnValue.fromString("O_060400347"));

        Filter filter2 = new Filter(Filter.CompareOperator.EQUAL, "from", ColumnValue.fromString("1546309320000"));

        List<Filter> subFilters = new ArrayList<Filter>();
        //subFilters.add(Filter.emptyFilter());
        subFilters.add(filter);
        subFilters.add(filter2);
        Filter filter3 = new Filter(Filter.LogicOperator.OR, subFilters);

        List<ITablestoreSplit> splits = manager.generateTablestoreSplits(ots, filter3, "timestream_share_car_order", para, columnToGet);
        TablestoreSplit anySplit = (TablestoreSplit) splits.get(0);
        System.out.println("count:" + splits.size());
        System.out.println("splitinfo:" + anySplit.toString());

        Iterator<Row> rows = anySplit.getRowIterator(ots);

        for (int i = 0; i < 100; i++) {
            if (rows.hasNext()) {
                Row row = rows.next();
                System.out.println("row:" + row.toString());
            } else {
                break;
            }
        }
    }
}
