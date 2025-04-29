package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.GeoHashPrecision;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.alicloud.openservices.tablestore.model.search.agg.SumAggregation;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField.Builder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.GroupKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.RowCountSort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alicloud.openservices.tablestore.core.protocol.SearchGroupByBuilder.buildPbGeoHashPrecision;
import static org.junit.Assert.assertEquals;

/**
 * GroupByBuilders Tester.
 *
 * @author <Authors xunjian>
 * @version 1.0
 * @since <pre>Sep 30, 2019</pre>
 */
public class GroupByBuildersTest extends BaseSearchTest {

    @Test
    public void testGroupByBuildersCount() {
        Method[] methods = GroupByBuilders.class.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getName().startsWith("groupBy")) {
                methodList.add(method);
            }
        }
        assertEquals("GroupByBuilders is incomplete", GroupByType.values().length, methodList.size());
    }

    @Test
    public void testType() {
        GroupByField groupByField = GroupByBuilders.groupByField("t", "t").build();
        assertEquals(GroupByType.GROUP_BY_FIELD, groupByField.getGroupByType());

        GroupByFilter groupByFilter = GroupByBuilders.groupByFilter("t").build();
        assertEquals(GroupByType.GROUP_BY_FILTER, groupByFilter.getGroupByType());

        GroupByGeoDistance groupByGeoDistance = GroupByBuilders.groupByGeoDistance("t", "t").build();
        assertEquals(GroupByType.GROUP_BY_GEO_DISTANCE, groupByGeoDistance.getGroupByType());

        GroupByRange groupByRange = GroupByBuilders.groupByRange("t", "t").build();
        assertEquals(GroupByType.GROUP_BY_RANGE, groupByRange.getGroupByType());

        GroupByHistogram groupByHistogram = GroupByBuilders.groupByHistogram("u", "u").build();
        assertEquals(GroupByType.GROUP_BY_HISTOGRAM, groupByHistogram.getGroupByType());

        GroupByDateHistogram groupByDateHistogram = GroupByBuilders.groupByDateHistogram("u", "u").build();
        assertEquals(GroupByType.GROUP_BY_DATE_HISTOGRAM, groupByDateHistogram.getGroupByType());

        GroupByComposite groupByComposite = GroupByBuilders.groupByComposite("u").build();
        assertEquals(GroupByType.GROUP_BY_COMPOSITE, groupByComposite.getGroupByType());
    }

    /**
     * Method: groupByField(String groupByName, String field)
     */
    @Test
    public void testGroupByField() throws Exception {
        GroupByField g1 = new GroupByField();
        g1.setFieldName("g1");
        g1.setGroupByName("n1");
        g1.setSize(10);

        GroupKeySort groupKeySort = new GroupKeySort();
        groupKeySort.setOrder(SortOrder.ASC);
        RowCountSort rowCountSort = new RowCountSort();
        rowCountSort.setOrder(SortOrder.DESC);
        GroupBySorter groupBySorter1 = new GroupBySorter();
        GroupBySorter groupBySorter2 = new GroupBySorter();
        groupBySorter1.setGroupKeySort(groupKeySort);
        groupBySorter2.setRowCountSort(rowCountSort);
        g1.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));
        g1.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
            AggregationBuilders.count("m2", "m2").build(),
            AggregationBuilders.count("m3", "m3").build()));

        Builder builder1 = GroupByBuilders.groupByField("n1", "g1")
            .size(10)
            .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()))
            .addSubAggregation(AggregationBuilders.max("m1", "m1"))
            .addSubAggregation(AggregationBuilders.count("m2", "m2"))
            .addSubAggregation(AggregationBuilders.count("m3", "m3"));

        Gson gson = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization().create();

        assertEquals(gson.toJson(builder1.build()), gson.toJson(g1));
    }

    /**
     * Method: groupByField(String groupByName, String field)
     */
    @Test
    public void testGroupByFieldWithDocCount() throws Exception {
        GroupByField g1 = new GroupByField();
        g1.setFieldName("g1");
        g1.setGroupByName("n1");
        g1.setSize(10);
        g1.setMinDocCount(10L);

        GroupKeySort groupKeySort = new GroupKeySort();
        groupKeySort.setOrder(SortOrder.ASC);
        RowCountSort rowCountSort = new RowCountSort();
        rowCountSort.setOrder(SortOrder.DESC);
        GroupBySorter groupBySorter1 = new GroupBySorter();
        GroupBySorter groupBySorter2 = new GroupBySorter();
        groupBySorter1.setGroupKeySort(groupKeySort);
        groupBySorter2.setRowCountSort(rowCountSort);
        g1.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));
        g1.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
            AggregationBuilders.count("m2", "m2").build(),
            AggregationBuilders.count("m3", "m3").build()));

        Builder builder1 = GroupByBuilders.groupByField("n1", "g1")
            .size(10)
            .minDocCount(10L)
            .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()))
            .addSubAggregation(AggregationBuilders.max("m1", "m1"))
            .addSubAggregation(AggregationBuilders.count("m2", "m2"))
            .addSubAggregation(AggregationBuilders.count("m3", "m3"));

        Gson gson = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization().create();

        assertEquals(gson.toJson(builder1.build()), gson.toJson(g1));
    }

    /**
     * Method: groupByHistogram(String groupByName, String field)
     */
    @Test
    public void testgroupByHistogram() throws Exception {
        {
            GroupByHistogram g1 = new GroupByHistogram();
            g1.setGroupByName("n1");
            g1.setFieldName("g1");
            g1.setInterval(ValueUtil.toColumnValue(5.0));
            g1.setMissing(new ColumnValue(1, ColumnType.INTEGER));
            g1.setOffset(ValueUtil.toColumnValue(1));
            g1.setMinDocCount(10L);
            g1.setFieldRange(
                new FieldRange(new ColumnValue(1.0, ColumnType.DOUBLE), new ColumnValue(5.0, ColumnType.DOUBLE)));
            GroupKeySort groupKeySort = new GroupKeySort();
            groupKeySort.setOrder(SortOrder.ASC);
            RowCountSort rowCountSort = new RowCountSort();
            rowCountSort.setOrder(SortOrder.DESC);
            GroupBySorter groupBySorter1 = new GroupBySorter();
            GroupBySorter groupBySorter2 = new GroupBySorter();
            groupBySorter1.setGroupKeySort(groupKeySort);
            groupBySorter2.setRowCountSort(rowCountSort);
            g1.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));

            GroupByHistogram.Builder builder1 = GroupByBuilders.groupByHistogram("n1", "g1")
                .interval(5.0)
                .missing(1)
                .offset(1)
                .minDocCount(10L)
                .addFieldRange(1.0, 5.0)
                .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()));

            Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .disableInnerClassSerialization()
                .serializeNulls()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder1.build()), gson.toJson(g1));
        }
        {
            GroupByHistogram g1 = new GroupByHistogram();
            g1.setGroupByName("n1");
            g1.setFieldName("g1");
            g1.setInterval(ValueUtil.toColumnValue(5.0));
            g1.setMissing(new ColumnValue(1, ColumnType.INTEGER));
            g1.setMinDocCount(10L);
            g1.setFieldRange(
                new FieldRange(new ColumnValue(1.0, ColumnType.DOUBLE), new ColumnValue(5.0, ColumnType.DOUBLE)));
            GroupKeySort groupKeySort = new GroupKeySort();
            groupKeySort.setOrder(SortOrder.ASC);
            RowCountSort rowCountSort = new RowCountSort();
            rowCountSort.setOrder(SortOrder.DESC);
            GroupBySorter groupBySorter1 = new GroupBySorter();
            GroupBySorter groupBySorter2 = new GroupBySorter();
            groupBySorter1.setGroupKeySort(groupKeySort);
            groupBySorter2.setRowCountSort(rowCountSort);
            g1.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));
            g1.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
                AggregationBuilders.count("m2", "m2").build(),
                AggregationBuilders.count("m3", "m3").build()));

            GroupByHistogram.Builder builder1 = GroupByBuilders.groupByHistogram("n1", "g1")
                .interval(5.0)
                .missing(1)
                .minDocCount(10L)
                .addFieldRange(1.0, 5.0)
                .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()))
                .addSubAggregation(AggregationBuilders.max("m1", "m1"))
                .addSubAggregation(AggregationBuilders.count("m2", "m2"))
                .addSubAggregation(AggregationBuilders.count("m3", "m3"));

            Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .disableInnerClassSerialization()
                .serializeNulls()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder1.build()), gson.toJson(g1));
        }
    }

    /**
     * test group by composite
     */
    @Test
    public void testGroupByComposite() {
        GroupByComposite groupByComposite = new GroupByComposite();
        groupByComposite.setGroupByName("groupByComposite");
        groupByComposite.setSize(1000);
        groupByComposite.setSources(Arrays.asList(
                GroupByBuilders.groupByField("groupByField", "field1").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).build(),
                GroupByBuilders.groupByHistogram("groupByHistogram", "field2").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).interval(50).build(),
                GroupByBuilders.groupByDateHistogram("groupByDateHistogram", "field3").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).interval(1, DateTimeUnit.DAY).build()));
        groupByComposite.setSubGroupBys(Arrays.asList(
                GroupByBuilders.groupByField("sub_groupByField", "field4")
                        .size(10)
                        .addGroupBySorter(Arrays.asList(
                                GroupBySorter.rowCountSortInDesc(),
                                GroupBySorter.groupKeySortInAsc())).build(),
                GroupByBuilders.groupByRange("sub_groupByRange", "field5")
                        .addRange(0.0, 100.0).build()));
        groupByComposite.setSubAggregations(Arrays.asList(
                AggregationBuilders.sum("subAgg", "field6").build(),
                AggregationBuilders.max("maxAgg", "field7").build()));

        GroupByComposite.Builder builder = GroupByBuilders.groupByComposite("groupByComposite");
        builder.size(1000);
        builder.setSources(Arrays.asList(
                GroupByBuilders.groupByField("groupByField", "field1").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).build(),
                GroupByBuilders.groupByHistogram("groupByHistogram", "field2").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).interval(50).build(),
                GroupByBuilders.groupByDateHistogram("groupByDateHistogram", "field3").addGroupBySorter(GroupBySorter.groupKeySortInDesc()).interval(1, DateTimeUnit.DAY).build()));
        builder.addSubGroupBy(GroupByBuilders.groupByField("sub_groupByField", "field4")
                .size(10)
                .addGroupBySorter(Arrays.asList(
                        GroupBySorter.rowCountSortInDesc(),
                        GroupBySorter.groupKeySortInAsc())).build());
        builder.addSubGroupBy(GroupByBuilders.groupByRange("sub_groupByRange", "field5")
                .addRange(0.0, 100.0).build());
        builder.addSubAggregation(AggregationBuilders.sum("subAgg", "field6").build());
        builder.addSubAggregation(AggregationBuilders.max("maxAgg", "field7").build());

        Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .disableInnerClassSerialization()
                .serializeNulls()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization().create();

        assertEquals(gson.toJson(builder.build()), gson.toJson(groupByComposite));
    }

    /**
     *  test group by date histogram
     */
    @Test
    public void testGroupByDateHistogram() {
        {
            GroupByDateHistogram groupByDateHistogram = new GroupByDateHistogram();
            groupByDateHistogram.setGroupByName("n1");
            groupByDateHistogram.setFieldName("g1");
            groupByDateHistogram.setInterval(1, DateTimeUnit.HOUR);
            groupByDateHistogram.setOffset(30, DateTimeUnit.MINUTE);
            groupByDateHistogram.setFieldRange(
                    new FieldRange(new ColumnValue(1, ColumnType.INTEGER), new ColumnValue(10, ColumnType.INTEGER)));
            groupByDateHistogram.setMissing(ValueUtil.toColumnValue(1));
            GroupKeySort groupKeySort = new GroupKeySort();
            groupKeySort.setOrder(SortOrder.ASC);
            RowCountSort rowCountSort = new RowCountSort();
            rowCountSort.setOrder(SortOrder.DESC);
            GroupBySorter groupBySorter1 = new GroupBySorter();
            GroupBySorter groupBySorter2 = new GroupBySorter();
            groupBySorter1.setGroupKeySort(groupKeySort);
            groupBySorter2.setRowCountSort(rowCountSort);
            groupByDateHistogram.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));
            groupByDateHistogram.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
                    AggregationBuilders.count("m2", "m2").build(),
                    AggregationBuilders.count("m3", "m3").build()));

            GroupByDateHistogram.Builder builder = GroupByBuilders.groupByDateHistogram("n1", "g1")
                    .interval(1, DateTimeUnit.HOUR)
                    .offset(30, DateTimeUnit.MINUTE)
                    .fieldRange(1, 10)
                    .missing(1)
                    .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()))
                    .addSubAggregation(AggregationBuilders.max("m1", "m1"))
                    .addSubAggregation(AggregationBuilders.count("m2", "m2"))
                    .addSubAggregation(AggregationBuilders.count("m3", "m3"));

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByDateHistogram));
        }
        {
            GroupByDateHistogram groupByDateHistogram = new GroupByDateHistogram();
            groupByDateHistogram.setGroupByName("n1");
            groupByDateHistogram.setFieldName("g1");
            groupByDateHistogram.setInterval(1, DateTimeUnit.HOUR);
            groupByDateHistogram.setOffset(30, DateTimeUnit.MILLISECOND);
            groupByDateHistogram.setFieldRange(
                    new FieldRange(new ColumnValue(1, ColumnType.INTEGER), new ColumnValue(10, ColumnType.INTEGER)));
            groupByDateHistogram.setMissing(ValueUtil.toColumnValue(1));
            GroupKeySort groupKeySort = new GroupKeySort();
            groupKeySort.setOrder(SortOrder.ASC);
            RowCountSort rowCountSort = new RowCountSort();
            rowCountSort.setOrder(SortOrder.DESC);
            GroupBySorter groupBySorter1 = new GroupBySorter();
            GroupBySorter groupBySorter2 = new GroupBySorter();
            groupBySorter1.setGroupKeySort(groupKeySort);
            groupBySorter2.setRowCountSort(rowCountSort);
            groupByDateHistogram.setGroupBySorters(Arrays.asList(groupBySorter1, groupBySorter2));
            groupByDateHistogram.setSubAggregations(Arrays.asList(AggregationBuilders.max("m1", "m1").build(),
                    AggregationBuilders.count("m2", "m2").build(),
                    AggregationBuilders.count("m3", "m3").build()));

            GroupByDateHistogram.Builder builder = GroupByBuilders.groupByDateHistogram("n1", "g1")
                    .interval(1, DateTimeUnit.HOUR)
                    .offset(30, DateTimeUnit.MILLISECOND)
                    .fieldRange(1, 10)
                    .missing(1)
                    .addGroupBySorter(Arrays.asList(GroupBySorter.groupKeySortInAsc(), GroupBySorter.rowCountSortInDesc()))
                    .addSubAggregation(AggregationBuilders.max("m1", "m1"))
                    .addSubAggregation(AggregationBuilders.count("m2", "m2"))
                    .addSubAggregation(AggregationBuilders.count("m3", "m3"));

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByDateHistogram));
        }
    }

    @Test
    public void testGroupByGeoGrid() {
        {
            GroupByGeoGrid groupByGeoGrid = new GroupByGeoGrid();
            groupByGeoGrid.setGroupByName("n1");
            groupByGeoGrid.setFieldName("g1");
            groupByGeoGrid.setSize(100);
            groupByGeoGrid.setPrecision(GeoHashPrecision.GHP_152M_152M_7);

            GroupByGeoGrid.Builder builder = GroupByBuilders.groupByGeoGrid("n1", "g1")
                    .size(100)
                    .precision(GeoHashPrecision.GHP_152M_152M_7);

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByGeoGrid));
        }
    }

    @Test
    public void testGroupByGeoGridWithSubGroupBy_1() {
        {
            GroupByGeoGrid groupByGeoGrid = new GroupByGeoGrid();
            groupByGeoGrid.setGroupByName("n1");
            groupByGeoGrid.setFieldName("g1");
            groupByGeoGrid.setSize(100);
            groupByGeoGrid.setPrecision(GeoHashPrecision.GHP_152M_152M_7);
            GroupBy subGroupBy = new GroupByField().setFieldName("g2").setGroupByName("n2");
            groupByGeoGrid.setSubGroupBys(Collections.singletonList(subGroupBy));

            GroupByGeoGrid.Builder builder = GroupByBuilders.groupByGeoGrid("n1", "g1")
                    .size(100)
                    .precision(GeoHashPrecision.GHP_152M_152M_7)
                    .addSubGroupBy(GroupByBuilders.groupByField("n2", "g2"));

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByGeoGrid));
        }
    }

    @Test
    public void testGroupByGeoGridWithSubGroupBy_2() {
        {
            GroupByGeoGrid groupByGeoGrid = new GroupByGeoGrid();
            groupByGeoGrid.setGroupByName("n1");
            groupByGeoGrid.setFieldName("g1");
            groupByGeoGrid.setSize(100);
            groupByGeoGrid.setPrecision(GeoHashPrecision.GHP_152M_152M_7);
            GroupBy subGroupBy = new GroupByField().setFieldName("g2").setGroupByName("n2");
            groupByGeoGrid.setSubGroupBys(Collections.singletonList(subGroupBy));

            GroupByGeoGrid.Builder builder = GroupByBuilders.groupByGeoGrid("n1", "g1")
                    .size(100)
                    .precision(GeoHashPrecision.GHP_152M_152M_7)
                    .addSubGroupBy(GroupByBuilders.groupByField("n2", "g2").build());

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByGeoGrid));
        }
    }

    @Test
    public void testGroupByGeoGridWithSubAgg_1() {
        {
            GroupByGeoGrid groupByGeoGrid = new GroupByGeoGrid();
            groupByGeoGrid.setGroupByName("n1");
            groupByGeoGrid.setFieldName("g1");
            groupByGeoGrid.setSize(100);
            groupByGeoGrid.setPrecision(GeoHashPrecision.GHP_152M_152M_7);
            Aggregation subAgg = new SumAggregation().setFieldName("g2").setAggName("a2");
            groupByGeoGrid.setSubAggregations(Collections.singletonList(subAgg));

            GroupByGeoGrid.Builder builder = GroupByBuilders.groupByGeoGrid("n1", "g1")
                    .size(100)
                    .precision(GeoHashPrecision.GHP_152M_152M_7)
                    .addSubAggregation(AggregationBuilders.sum("a2", "g2"));

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByGeoGrid));
        }
    }

    @Test
    public void testGroupByGeoGridWithSubAgg_2() {
        {
            GroupByGeoGrid groupByGeoGrid = new GroupByGeoGrid();
            groupByGeoGrid.setGroupByName("n1");
            groupByGeoGrid.setFieldName("g1");
            groupByGeoGrid.setSize(100);
            groupByGeoGrid.setPrecision(GeoHashPrecision.GHP_152M_152M_7);
            Aggregation subAgg = new SumAggregation().setFieldName("g2").setAggName("a2");
            groupByGeoGrid.setSubAggregations(Collections.singletonList(subAgg));

            GroupByGeoGrid.Builder builder = GroupByBuilders.groupByGeoGrid("n1", "g1")
                    .size(100)
                    .precision(GeoHashPrecision.GHP_152M_152M_7)
                    .addSubAggregation(AggregationBuilders.sum("a2", "g2").build());

            Gson gson = new GsonBuilder()
                    .disableHtmlEscaping()
                    .disableInnerClassSerialization()
                    .serializeNulls()
                    .serializeSpecialFloatingPointValues()
                    .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(builder.build()), gson.toJson(groupByGeoGrid));
        }
    }

    @Test
    public void testBuildPbGeoHashPrecision() {
        GeoHashPrecision[] precision = {GeoHashPrecision.GHP_5009KM_4992KM_1, GeoHashPrecision.GHP_1252KM_624KM_2, GeoHashPrecision.GHP_156KM_156KM_3,
                GeoHashPrecision.GHP_39KM_19KM_4, GeoHashPrecision.GHP_4900M_4900M_5, GeoHashPrecision.GHP_1200M_609M_6, GeoHashPrecision.GHP_152M_152M_7,
                GeoHashPrecision.GHP_38M_19M_8, GeoHashPrecision.GHP_480CM_480CM_9, GeoHashPrecision.GHP_120CM_595MM_10, GeoHashPrecision.GHP_149MM_149MM_11,
                GeoHashPrecision.GHP_37MM_19MM_12};

        Search.GeoHashPrecision[] pbPrecision = {Search.GeoHashPrecision.GHP_5009KM_4992KM_1, Search.GeoHashPrecision.GHP_1252KM_624KM_2, Search.GeoHashPrecision.GHP_156KM_156KM_3,
                Search.GeoHashPrecision.GHP_39KM_19KM_4, Search.GeoHashPrecision.GHP_4900M_4900M_5, Search.GeoHashPrecision.GHP_1200M_609M_6, Search.GeoHashPrecision.GHP_152M_152M_7,
                Search.GeoHashPrecision.GHP_38M_19M_8, Search.GeoHashPrecision.GHP_480CM_480CM_9, Search.GeoHashPrecision.GHP_120CM_595MM_10, Search.GeoHashPrecision.GHP_149MM_149MM_11,
                Search.GeoHashPrecision.GHP_37MM_19MM_12};

        for (int i = 0; i < 12; i++) {
            assertEquals(pbPrecision[i], buildPbGeoHashPrecision(precision[i]));
        }
    }
}
