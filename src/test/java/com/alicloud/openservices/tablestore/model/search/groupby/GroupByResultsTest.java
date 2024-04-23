package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.GeoGrid;
import com.alicloud.openservices.tablestore.model.search.GeoPoint;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResult;
import com.alicloud.openservices.tablestore.model.search.agg.AggregationResults;
import com.alicloud.openservices.tablestore.model.search.agg.MaxAggregationResult;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GroupByResultsTest extends BaseSearchTest {

    @Test
    public void testGroupByResultsCount() {
        Method[] methods = GroupByResults.class.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getName().startsWith("getAsGroupBy")) {
                methodList.add(method);
            }
        }
        assertEquals("GroupByResults is incomplete", GroupByType.values().length, methodList.size());
    }


    @Test
    public void testGetAsGroupByHistogramResult() {
        {
            GroupByResults groupByResults = new GroupByResults();
            GroupByHistogramItem item = new GroupByHistogramItem();
            item.setKey(ColumnValue.fromLong(1));
            List<GroupByHistogramItem> groupByHistogramItems = new ArrayList<GroupByHistogramItem>();
            groupByHistogramItems.add(item);
            item.setValue(12L);
            Map<String, GroupByResult> map = new HashMap<String, GroupByResult>();
            map.put("8h", new GroupByHistogramResult().setGroupByName("8h").setGroupByHistogramItems(groupByHistogramItems));
            groupByResults.setGroupByResultMap(map);

            assertEquals(1, groupByResults.getAsGroupByHistogramResult("8h").getGroupByHistogramItems().size(), 0.000001);
            assertEquals(GroupByType.GROUP_BY_HISTOGRAM, groupByResults.getAsGroupByHistogramResult("8h").getGroupByType());
            assertEquals("8h", groupByResults.getAsGroupByHistogramResult("8h").getGroupByName());
        }
        {
            GroupByResults groupByResults = new GroupByResults();
            Map<String, GroupByResult> map = new HashMap<String, GroupByResult>();
            map.put("8h", new GroupByRangeResult().setGroupByName("8h"));
            groupByResults.setGroupByResultMap(map);

            Throwable t = null;
            try {
                groupByResults.getAsGroupByHistogramResult("8h");
            } catch (Exception ex) {
                t = ex;
            }
            assertNotNull(t);
            assertEquals("the result with this groupByName can't cast to GroupByHistogramResult.", t.getMessage());
            assertEquals(IllegalArgumentException.class, t.getClass());
        }
    }

    @Test
    public void testGetAsGroupByGeoGridResult() {
        {
            String groupByName = "groupBy";
            GeoGrid geoGrid = new GeoGrid(new GeoPoint(1, 1), new GeoPoint(2, 2));

            GroupByResults groupByResults = new GroupByResults();
            GroupByGeoGridResultItem item = new GroupByGeoGridResultItem();
            item.setKey("a");
            item.setGeoGrid(geoGrid);
            item.setRowCount(1L);

            GroupByResults subGroupByResults = new GroupByResults();
            GroupByFieldResultItem subItem = new GroupByFieldResultItem();
            subItem.setKey("subField");
            subItem.setRowCount(2L);
            List<GroupByFieldResultItem> subResultItems = Collections.singletonList(subItem);
            Map<String, GroupByResult> subGroupByResultMap = new HashMap<String, GroupByResult>();
            subGroupByResultMap.put("subGroupBy", new GroupByFieldResult().setGroupByName("subGroupBy").setGroupByFieldResultItems(subResultItems));
            subGroupByResults.setGroupByResultMap(subGroupByResultMap);
            item.setSubGroupByResults(subGroupByResults);

            AggregationResults subAggResults = new AggregationResults();
            MaxAggregationResult subMaxAgg = new MaxAggregationResult();
            subMaxAgg.setAggName("subAgg");
            subMaxAgg.setValue(10);
            Map<String, AggregationResult> aggMap = new HashMap<String, AggregationResult>();
            aggMap.put("subAgg", subMaxAgg);
            subAggResults.setResultMap(aggMap);
            item.setSubAggregationResults(subAggResults);

            List<GroupByGeoGridResultItem> groupByGeoGridResultItems = new ArrayList<GroupByGeoGridResultItem>();
            groupByGeoGridResultItems.add(item);
            Map<String, GroupByResult> groupByResultMap = new HashMap<String, GroupByResult>();
            groupByResultMap.put(groupByName, new GroupByGeoGridResult().setGroupByName(groupByName).setGroupByGeoGridResultItems(groupByGeoGridResultItems));
            groupByResults.setGroupByResultMap(groupByResultMap);

            assertEquals(1, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().size(), 0.000001);
            assertEquals(GroupByType.GROUP_BY_GEO_GRID, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByType());
            assertEquals(groupByName, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByName());
            assertEquals("a", groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getKey());
            assertEquals(geoGrid, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getGeoGrid());
            assertEquals(1, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getRowCount());
            assertEquals("subField", groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getSubGroupByResults().
                    getAsGroupByFieldResult("subGroupBy").getGroupByFieldResultItems().get(0).getKey());
            assertEquals(2, groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getSubGroupByResults().
                    getAsGroupByFieldResult("subGroupBy").getGroupByFieldResultItems().get(0).getRowCount());
            assertEquals("subAgg", groupByResults.getAsGroupByGeoGridResult(groupByName).getGroupByGeoGridResultItems().get(0).getSubAggregationResults().
                    getAsMaxAggregationResult("subAgg").getAggName());
        }
    }
}