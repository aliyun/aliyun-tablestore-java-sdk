package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Arrays;

import com.alicloud.openservices.tablestore.model.search.agg.AggregationBuilders;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByField.Builder;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.GroupKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.RowCountSort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * GroupByBuilders Tester.
 *
 * @author <Authors xunjian>
 * @version 1.0
 * @since <pre>9月 30, 2019</pre>
 */
public class GroupByBuildersTest extends BaseSearchTest {

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
} 
