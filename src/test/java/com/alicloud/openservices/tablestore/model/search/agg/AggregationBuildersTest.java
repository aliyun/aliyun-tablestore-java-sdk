package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.ValueUtil;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort.Sorter;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AggregationBuildersTest extends BaseSearchTest {

    @Test
    public void testQueryBuildersCount() {
        Method[] methods = AggregationBuilders.class.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getReturnType().getName().endsWith("$Builder")) {
                methodList.add(method);
            }
        }
        assertEquals("AggregationBuilders is incomplete", AggregationType.values().length, methodList.size());
    }

    @Test
    public void max() {
        Aggregation aggregation = AggregationBuilders.max("a", "b").missing(12).build();
        assertEquals(AggregationType.AGG_MAX, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((MaxAggregation)aggregation).getFieldName());
        assertEquals(12L, ValueUtil.toObject(((MaxAggregation)aggregation).getMissing()));
    }

    @Test
    public void min() {
        Aggregation aggregation = AggregationBuilders.min("a", "b").missing(12).build();
        assertEquals(AggregationType.AGG_MIN, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((MinAggregation)aggregation).getFieldName());
        assertEquals(12L, ValueUtil.toObject(((MinAggregation)aggregation).getMissing()));
    }

    @Test
    public void sum() {
        Aggregation aggregation = AggregationBuilders.sum("a", "b").missing(12).build();
        assertEquals(AggregationType.AGG_SUM, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((SumAggregation)aggregation).getFieldName());
        assertEquals(12L, ValueUtil.toObject(((SumAggregation)aggregation).getMissing()));
    }

    @Test
    public void avg() {
        Aggregation aggregation = AggregationBuilders.avg("a", "b").missing(12).build();
        assertEquals(AggregationType.AGG_AVG, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((AvgAggregation)aggregation).getFieldName());
        assertEquals(12L, ValueUtil.toObject(((AvgAggregation)aggregation).getMissing()));
    }

    @Test
    public void distinctCount() {
        Aggregation aggregation = AggregationBuilders.distinctCount("a", "b").missing(12).build();
        assertEquals(AggregationType.AGG_DISTINCT_COUNT, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((DistinctCountAggregation)aggregation).getFieldName());
        assertEquals(12L, ValueUtil.toObject(((DistinctCountAggregation)aggregation).getMissing()));
    }

    @Test
    public void count() {
        Aggregation aggregation = AggregationBuilders.count("a", "b").build();
        assertEquals(AggregationType.AGG_COUNT, aggregation.getAggType());
        assertEquals("a", aggregation.getAggName());
        assertEquals("b", ((CountAggregation)aggregation).getFieldName());
    }

    @Test
    public void topRows() {
        {
            Aggregation aggregation = AggregationBuilders.topRows("a")
                .limit(10)
                .sort(new Sort(Collections.<Sorter>singletonList(new FieldSort("f1", SortOrder.DESC))))
                .build();
            assertEquals(AggregationType.AGG_TOP_ROWS, aggregation.getAggType());
            assertEquals("a", aggregation.getAggName());
            TopRowsAggregation topRowsAggregation = (TopRowsAggregation)aggregation;
            assertEquals("a", topRowsAggregation.getAggName());
            assertEquals(10, topRowsAggregation.getLimit().intValue());
            assertEquals("f1", ((FieldSort)topRowsAggregation.getSort().getSorters().get(0)).getFieldName());
            assertEquals(SortOrder.DESC, ((FieldSort)topRowsAggregation.getSort().getSorters().get(0)).getOrder());
        }
        {
            Aggregation aggregation = AggregationBuilders.topRows("a")
                .limit(99)
                .build();
            assertEquals(AggregationType.AGG_TOP_ROWS, aggregation.getAggType());
            assertEquals("a", aggregation.getAggName());
            TopRowsAggregation topRowsAggregation = (TopRowsAggregation)aggregation;
            assertEquals("a", topRowsAggregation.getAggName());
            assertEquals(99, topRowsAggregation.getLimit().intValue());
            assertNull(topRowsAggregation.getSort());
        }
        {
            TopRowsAggregation ag1 = new TopRowsAggregation();
            ag1.setAggName("aggName");
            ag1.setLimit(10);
            ColumnsToGet columnsToGet = new ColumnsToGet();
            columnsToGet.setColumns(Arrays.asList("12","23"));
            ag1.setSort(new Sort(Collections.<Sorter>singletonList(new FieldSort("f1", SortOrder.DESC))));

            TopRowsAggregation ag2 = AggregationBuilders.topRows("aggName")
                .limit(10)
                .sort(new Sort(Collections.<Sorter>singletonList(new FieldSort("f1", SortOrder.DESC))))
                .build();
            Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .disableInnerClassSerialization()
                .serializeNulls()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(ag1), gson.toJson(ag2));
        }
    }

    @Test
    public void percentiles() {
        {
            Aggregation aggregation = AggregationBuilders.percentiles("a","bb")
                .percentiles(Collections.<Double>singletonList(99.0))
                .missing(1L)
                .build();
            assertEquals(AggregationType.AGG_PERCENTILES, aggregation.getAggType());
            assertEquals("a", aggregation.getAggName());
            assertEquals(1L, ((PercentilesAggregation)aggregation).getMissing().getValue());
            assertEquals("bb", ((PercentilesAggregation)aggregation).getFieldName());
            assertTrue(((Double)((PercentilesAggregation)aggregation).getPercentiles().get(0)).compareTo(99.0) == 0);
        }
        {
            Aggregation aggregation = AggregationBuilders.percentiles("a","bb")
                .fieldName("bb")
                .build();
            assertEquals(AggregationType.AGG_PERCENTILES, aggregation.getAggType());
            assertEquals("a", aggregation.getAggName());
            assertEquals("bb", ((PercentilesAggregation)aggregation).getFieldName());
            assertNull(((PercentilesAggregation)aggregation).getPercentiles());
        }
        {
            Aggregation agg1 = AggregationBuilders.percentiles("a","bb")
                .fieldName("bb")
                .percentiles(Arrays.asList(0.0,50.0,99.0))
                .build();
            PercentilesAggregation agg2=new PercentilesAggregation();
            agg2.setAggName("a");
            agg2.setFieldName("bb");
            List<Double> percentiles=Arrays.asList(0.0,50.0,99.0);
            agg2.setPercentiles(percentiles);

            Gson gson = new GsonBuilder()
                .disableHtmlEscaping()
                .disableInnerClassSerialization()
                .serializeNulls()
                .serializeSpecialFloatingPointValues()
                .enableComplexMapKeySerialization().create();

            assertEquals(gson.toJson(agg1), gson.toJson(agg2));
        }
    }
}