package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryType;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class testTablestoreSplit {
    @Test
    public void testBuildMultiTreeFilter() {
        Filter binaryFilter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "a", ColumnValue.fromLong(1)),
                        new Filter(Filter.LogicOperator.AND, Arrays.asList(
                                new Filter(Filter.CompareOperator.LESS_THAN, "a", ColumnValue.fromLong(100)),
                                new Filter(Filter.CompareOperator.LESS_THAN, "a", ColumnValue.fromLong(50))))
                )),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_EQUAL, "a", ColumnValue.fromLong(2)),
                        new Filter(Filter.LogicOperator.OR, Arrays.asList(
                                new Filter(Filter.CompareOperator.GREATER_THAN, "c", ColumnValue.fromLong(666)),
                                new Filter(Filter.CompareOperator.LESS_THAN, "d", ColumnValue.fromLong(2))
                        )))
                )));
        TablestoreSplit split = new TablestoreSplit(null, null, null);
        Filter multiTreeFilter = split.buildFlatTreeFilter(binaryFilter);
        // TODO: Finish assert
        Assert.assertEquals(5, multiTreeFilter.getSubFilters().size());
    }

    @Test
    public void testMergedSearchQueryFromFilter() {
        Filter binaryFilter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "a", ColumnValue.fromLong(2)),
                        new Filter(Filter.LogicOperator.AND, Arrays.asList(
                                new Filter(Filter.CompareOperator.LESS_THAN, "a", ColumnValue.fromLong(50)),
                                new Filter(Filter.CompareOperator.LESS_EQUAL, "a", ColumnValue.fromLong(50))))
                )),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_EQUAL, "a", ColumnValue.fromLong(2)),
                        new Filter(Filter.LogicOperator.OR, Arrays.asList(
                                new Filter(Filter.CompareOperator.GREATER_THAN, "c", ColumnValue.fromLong(666)),
                                new Filter(Filter.CompareOperator.LESS_THAN, "d", ColumnValue.fromLong(2))
                        )))
                )));
        TablestoreSplit split = new TablestoreSplit(null, null, null);
        Filter multiTreeFilter = split.buildFlatTreeFilter(binaryFilter);
        Query query = null;
        try {
            query = split.buildMergedSearchQueryFromFilter(multiTreeFilter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // TODO : Finish assert
        Assert.assertEquals(QueryType.QueryType_BoolQuery, query.getQueryType());
    }

    @Test
    public void testGeneratePkRangeWithOnePK() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        PrimaryKey expectedBegin = expected.build();

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        PrimaryKey expectedEnd = expected.build();

        // > && <
        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.LESS_THAN, "pk1", ColumnValue.fromLong(2))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(expectedBegin, ranges.get(0).getBegin());
        Assert.assertEquals(expectedEnd, ranges.get(0).getEnd());

        // >= && <=
        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(2))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(expectedBegin, ranges.get(0).getBegin());
        Assert.assertEquals(expectedEnd, ranges.get(0).getEnd());
        Assert.assertEquals(expectedEnd, ranges.get(1).getEqual());

        // >= && <
        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.LESS_THAN, "pk1", ColumnValue.fromLong(2))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(expectedBegin, ranges.get(0).getBegin());
        Assert.assertEquals(expectedEnd, ranges.get(0).getEnd());

        // > && <=
        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(2))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(2, ranges.size());
        Assert.assertEquals(expectedBegin, ranges.get(0).getBegin());
        Assert.assertEquals(expectedEnd, ranges.get(0).getEnd());
        Assert.assertEquals(expectedEnd, ranges.get(1).getEqual());

        // =
        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());

        Assert.assertEquals(1, ranges.size());
        Assert.assertEquals(expectedBegin, ranges.get(0).getEqual());
    }

    @Test
    public void testGeneratePkRangeWithOnePKInvalidRange() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);

        // invalid range: > 4 && <= 2
        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(4)),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(2))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(0, ranges.size());
    }

    @Test
    public void testGeneratePkRangeWithOnePKWithOrFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);

        // [INF_MIN, INF_MAX) with pk1 = 1 or pk1 = 16
        Filter filter = new Filter(Filter.LogicOperator.OR, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(16))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(2, ranges.size());
        PrimaryKeyBuilder beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        Assert.assertEquals(beginPK.build(), ranges.get(0).getEqual());
        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(16));
        Assert.assertEquals(beginPK.build(), ranges.get(1).getEqual());

        // [1, 16) with pk1 = 1 or pk1 = 16
        beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(16));

        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());
        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        Assert.assertEquals(beginPK.build(), ranges.get(0).getEqual());
    }

    @Test
    public void testGeneratePkRangeWithOnePKMultiRanges() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.INF_MAX);

        // filter: (pk1 > 2 & pk1 < 4) or (pk1 >= 4 and pk1 < 6) or (pk1 > 8 and pk1 <= 10)
        Filter filter = new Filter(Filter.LogicOperator.OR, Arrays.asList(
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(2)),
                        new Filter(Filter.CompareOperator.LESS_THAN, "pk1", ColumnValue.fromLong(4)))),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk1", ColumnValue.fromLong(4)),
                        new Filter(Filter.CompareOperator.LESS_THAN, "pk1", ColumnValue.fromLong(6)))),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(8)),
                        new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(10))))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(4, ranges.size());

        PrimaryKeyBuilder beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        PrimaryKeyBuilder endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(4));
        Assert.assertEquals(beginPK.build(), ranges.get(0).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(0).getEnd());

        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(4));
        endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(6));
        Assert.assertEquals(beginPK.build(), ranges.get(1).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(1).getEnd());

        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(8));
        endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(10));
        Assert.assertEquals(beginPK.build(), ranges.get(2).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(2).getEnd());

        endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(10));
        Assert.assertEquals(endPK.build(), ranges.get(3).getEqual());
    }

    @Test
    public void testGeneratePkRangeWithPrefixMatch() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        // with 1 prefix match
        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());


        // with 2 prefix match
        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(2))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());
    }

    @Test
    public void testGeneratePkRangeWithAllEqualFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(2)),
                new Filter(Filter.CompareOperator.EQUAL, "pk3", ColumnValue.fromString("testpk3"))));
        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("testpk3"));
        Assert.assertEquals(expected.build(), ranges.get(0).getEqual());
    }

    @Test
    public void testGeneratePkRangeWithEqualAndMiddleRangeFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk2", ColumnValue.fromLong(2)),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk2", ColumnValue.fromLong(3)),
                new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk3", ColumnValue.fromString("testpk3")),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk3", ColumnValue.fromString("testpk4"))));
        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());
        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(3));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());
    }

    @Test
    public void testGeneratePkRangeWithEqualAndRangeFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(2)),
                new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk3", ColumnValue.fromString("testpk3")),
                new Filter(Filter.CompareOperator.LESS_EQUAL, "pk3", ColumnValue.fromString("testpk4"))));
        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(2, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("testpk3"));
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());
        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString("testpk4"));
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());

        Assert.assertEquals(expected.build(), ranges.get(1).getEqual());
    }

    @Test
    public void testGeneratePkRangeMultiRanges() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(0));
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(9));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        // filter: (pk1 > 2 & pk1 <= 4) or (pk1 >= 4 and pk1 < 6) or (pk1 > 8 and pk1 <= 10)
        Filter filter = new Filter(Filter.LogicOperator.OR, Arrays.asList(
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(2)),
                        new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(4)))),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_EQUAL, "pk1", ColumnValue.fromLong(4)),
                        new Filter(Filter.CompareOperator.LESS_THAN, "pk1", ColumnValue.fromLong(6)))),
                new Filter(Filter.LogicOperator.AND, Arrays.asList(
                        new Filter(Filter.CompareOperator.GREATER_THAN, "pk1", ColumnValue.fromLong(8)),
                        new Filter(Filter.CompareOperator.LESS_EQUAL, "pk1", ColumnValue.fromLong(10))))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(3, ranges.size());

        PrimaryKeyBuilder beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(2));
        beginPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        PrimaryKeyBuilder endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(4));
        endPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        endPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(beginPK.build(), ranges.get(0).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(0).getEnd());

        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(4));
        beginPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(6));
        endPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(beginPK.build(), ranges.get(1).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(1).getEnd());

        beginPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(8));
        beginPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        endPK = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPK.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(9));
        endPK.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endPK.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(beginPK.build(), ranges.get(2).getBegin());
        Assert.assertEquals(endPK.build(), ranges.get(2).getEnd());
    }

    @Test
    public void testCheckIfMatchTheFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        Split kvSplit = new Split();
        PrimaryKeyBuilder begin = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        PrimaryKeyBuilder end = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        begin.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(10));
        end.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(20));
        kvSplit.setLowerBound(begin.build());
        kvSplit.setUpperBound(end.build());
        split.setKvSplit(kvSplit);

        Filter filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1))));
        Assert.assertFalse(split.checkIfMatchTheFilter(filter));

        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(10))));
        Assert.assertTrue(split.checkIfMatchTheFilter(filter));

        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(15))));
        Assert.assertTrue(split.checkIfMatchTheFilter(filter));

        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(20))));
        Assert.assertFalse(split.checkIfMatchTheFilter(filter));

        filter = new Filter(Filter.LogicOperator.AND, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(21))));
        Assert.assertFalse(split.checkIfMatchTheFilter(filter));
    }

    @Test
    public void testGeneratePkRangeWithOrFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(16));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        // pk1 = 1 or pk1 = 15
        Filter filter = new Filter(Filter.LogicOperator.OR, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(15))));
        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MAX);
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());


        // pk1 = 14 or pk1 = 16
        filter = new Filter(Filter.LogicOperator.OR, Arrays.asList(
                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(14)),
                new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(16))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(0, ranges.size());
    }

    @Test
    public void testGeneratePkRangeWithPrefixOrFilter() {
        TablestoreSplit split = new TablestoreSplit(TablestoreSplit.SplitType.KeyValue, null, null);
        TableMeta meta = new TableMeta("testTable");
        meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
        meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
        split.setKvTableMeta(meta);

        PrimaryKeyBuilder beginBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        beginBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        beginBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        beginBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        PrimaryKeyBuilder endBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endBuilder.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(16));
        endBuilder.addPrimaryKeyColumn("pk2", PrimaryKeyValue.INF_MIN);
        endBuilder.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);

        // (pk1 = 1 or pk1 = 15) and pk2 = 2
        Filter filter = new Filter(Filter.LogicOperator.AND,
                Arrays.asList(new Filter(Filter.LogicOperator.OR, Arrays.asList(
                        new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                        new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(15)))),
                        new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(2))));

        List<TablestoreSplit.PkRange> ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        PrimaryKeyBuilder expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());


        // pk2 = 2 and (pk1 = 1 or pk1 = 15)
        filter = new Filter(Filter.LogicOperator.AND,
                Arrays.asList(
                        new Filter(Filter.CompareOperator.EQUAL, "pk2", ColumnValue.fromLong(2)),
                        new Filter(Filter.LogicOperator.OR, Arrays.asList(
                                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(1)),
                                new Filter(Filter.CompareOperator.EQUAL, "pk1", ColumnValue.fromLong(15))))));
        ranges = split.generatePkRange(filter, beginBuilder.build(), endBuilder.build());
        Assert.assertEquals(1, ranges.size());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MIN);
        Assert.assertEquals(expected.build(), ranges.get(0).getBegin());

        expected = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        expected.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(15));
        expected.addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(2));
        expected.addPrimaryKeyColumn("pk3", PrimaryKeyValue.INF_MAX);
        Assert.assertEquals(expected.build(), ranges.get(0).getEnd());
    }
}
