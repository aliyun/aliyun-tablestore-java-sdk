package com.alicloud.openservices.tablestore.model.search.agg;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.Row;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AggregationResultsTest extends BaseSearchTest {

    AggregationResults aggregationResults;

    @Before
    public void init() {
        aggregationResults = new AggregationResults();
        Map<String, AggregationResult> map = new HashMap<String, AggregationResult>();
        map.put("1a", new AvgAggregationResult().setValue(123).setAggName("1a"));
        map.put("2b", new SumAggregationResult().setValue(456).setAggName("2b"));
        map.put("3c", new MinAggregationResult().setValue(789).setAggName("3c"));
        map.put("4d", new MaxAggregationResult().setValue(101112).setAggName("4d"));
        map.put("5e", new CountAggregationResult().setValue(131415).setAggName("5e"));
        map.put("6f", new DistinctCountAggregationResult().setValue(161718).setAggName("6f"));
        map.put("7g", new TopRowsAggregationResult().setAggName("7g").setRows(Collections.<Row>emptyList()));
        map.put("8h", new PercentilesAggregationResult().setAggName("8h").setPercentilesAggregationItems(Collections.<PercentilesAggregationItem>emptyList()));

        aggregationResults.setResultMap(map);
    }

    @Test
    public void testQueryBuildersCount() {
        Method[] methods = AggregationResults.class.getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getName().startsWith("getAs")) {
                methodList.add(method);
            }
        }
        assertEquals("AggregationResults is incomplete", AggregationType.values().length, methodList.size());
    }

    @Test
    public void size() {
        assertEquals(0, new AggregationResults().size());
        assertEquals(8, aggregationResults.size());

    }

    @Test
    public void getAsAvgAggregationResult() {
        assertEquals(123, aggregationResults.getAsAvgAggregationResult("1a").getValue(), 0.000001);
        assertEquals(AggregationType.AGG_AVG, aggregationResults.getAsAvgAggregationResult("1a").getAggType());
        assertEquals("1a", aggregationResults.getAsAvgAggregationResult("1a").getAggName());

    }

    @Test
    public void getAsDistinctCountAggregationResult() {
        assertEquals(161718, aggregationResults.getAsDistinctCountAggregationResult("6f").getValue(), 0.000001);
        assertEquals(AggregationType.AGG_DISTINCT_COUNT,
            aggregationResults.getAsDistinctCountAggregationResult("6f").getAggType());
        assertEquals("6f", aggregationResults.getAsDistinctCountAggregationResult("6f").getAggName());
    }

    @Test
    public void getAsMaxAggregationResult() {
        assertEquals(101112, aggregationResults.getAsMaxAggregationResult("4d").getValue(), 0.000001);
        assertEquals(AggregationType.AGG_MAX, aggregationResults.getAsMaxAggregationResult("4d").getAggType());
        assertEquals("4d", aggregationResults.getAsMaxAggregationResult("4d").getAggName());

        Throwable t = null;
        try {
            aggregationResults.getAsMinAggregationResult("4d");
        } catch (Exception ex) {
            t = ex;
        }
        assertNotNull(t);
        assertEquals("the result with this aggregationName can't cast to MinAggregationResult.", t.getMessage());
        assertEquals(IllegalArgumentException.class, t.getClass());

    }

    @Test
    public void getAsTopRowsAggregationResult() {
        assertEquals(0, aggregationResults.getAsTopRowsAggregationResult("7g").getRows().size(), 0.000001);
        assertEquals(AggregationType.AGG_TOP_ROWS, aggregationResults.getAsTopRowsAggregationResult("7g").getAggType());
        assertEquals("7g", aggregationResults.getAsTopRowsAggregationResult("7g").getAggName());

        Throwable t = null;
        try {
            aggregationResults.getAsTopRowsAggregationResult("1a");
        } catch (Exception ex) {
            t = ex;
        }
        assertNotNull(t);
        assertEquals("the result with this aggregationName can't cast to TopRowsAggregationResult.", t.getMessage());
        assertEquals(IllegalArgumentException.class, t.getClass());

    }

    @Test
    public void getAsPercentilesAggregationResult() {
        assertEquals(0, aggregationResults.getAsPercentilesAggregationResult("8h").getPercentilesAggregationItems().size(), 0.000001);
        assertEquals(AggregationType.AGG_PERCENTILES, aggregationResults.getAsPercentilesAggregationResult("8h").getAggType());
        assertEquals("8h", aggregationResults.getAsPercentilesAggregationResult("8h").getAggName());

        Throwable t = null;
        try {
            aggregationResults.getAsPercentilesAggregationResult("1a");
        } catch (Exception ex) {
            t = ex;
        }
        assertNotNull(t);
        assertEquals("the result with this aggregationName can't cast to PercentilesAggregationResult.", t.getMessage());
        assertEquals(IllegalArgumentException.class, t.getClass());

    }

}