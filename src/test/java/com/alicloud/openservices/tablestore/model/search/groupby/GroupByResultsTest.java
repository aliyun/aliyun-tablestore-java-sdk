package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
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
}