package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestRowQueryCriteria {
    @Test
    public void testConstructor() {
        RowQueryCriteria criteria = new RowQueryCriteria("T");
        assertEquals(criteria.getTableName(), "T");
        assertTrue(!criteria.hasSetTimeRange());
        assertTrue(!criteria.hasSetMaxVersions());
        assertTrue(criteria.getColumnsToGet().isEmpty());
        assertEquals(criteria.numColumnsToGet(), 0);

        try {
            new RowQueryCriteria(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testGetSetMaxVersions() {
        RowQueryCriteria criteria = new RowQueryCriteria("T");
        assertTrue(!criteria.hasSetMaxVersions());
        try {
            criteria.getMaxVersions();
            fail();
        } catch (IllegalStateException e) {

        }

        try {
            criteria.setMaxVersions(0);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            criteria.setMaxVersions(-1);
            fail();
        } catch (IllegalArgumentException e) {

        }

        criteria.setMaxVersions(10);
        assertEquals(criteria.getMaxVersions(), 10);
    }

    @Test
    public void testGetSetTimeRange() {
        RowQueryCriteria criteria = new RowQueryCriteria("T");
        assertTrue(!criteria.hasSetTimeRange());
        try {
            criteria.getTimeRange();
            fail();
        } catch (IllegalStateException e) {

        }

        try {
            criteria.setTimeRange(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            criteria.setTimestamp(-1);
            fail();
        } catch (IllegalArgumentException e) {

        }

        TimeRange timeRange = new TimeRange(1418380771, 1418390771);
        criteria.setTimeRange(timeRange);
        assertEquals(criteria.getTimeRange(), timeRange);

        criteria.setTimestamp(1418380771);
        assertEquals(criteria.getTimeRange(), new TimeRange(1418380771, 1418380772));
    }

    @Test
    public void testGetSetFilter() {
        RowQueryCriteria criteria = new RowQueryCriteria("T");
        assertTrue(!criteria.hasSetFilter());
        try {
            criteria.getFilter();
            fail();
        } catch (IllegalStateException e) {

        }

        try {
            criteria.setFilter(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        SingleColumnValueFilter filter =  new SingleColumnValueFilter("column_1",
                SingleColumnValueFilter.CompareOperator.GREATER_THAN, ColumnValue.fromLong(10));
        criteria.setFilter(filter);

        assertTrue(criteria.getFilter() != null);
        Filter tmp = criteria.getFilter();
        assertTrue(tmp instanceof SingleColumnValueFilter);
        assertEquals(((SingleColumnValueFilter) tmp).getColumnValue(), ColumnValue.fromLong(10));
        assertEquals(((SingleColumnValueFilter)tmp).getColumnName(), "column_1");
        assertEquals(((SingleColumnValueFilter)tmp).getOperator(), SingleColumnValueFilter.CompareOperator.GREATER_THAN);
    }

    @Test
    public void testGetSetColumnsToGet() {
        RowQueryCriteria criteria = new RowQueryCriteria("T");

        assertTrue(criteria.getColumnsToGet().isEmpty());
        assertEquals(criteria.numColumnsToGet(), 0);

        criteria.addColumnsToGet("Column1");
        assertEquals(criteria.numColumnsToGet(), 1);
        assertTrue(criteria.getColumnsToGet().contains("Column1"));

        String[] columns = new String[4];
        columns[0] = "Column2";
        columns[1] = "Column3";
        columns[2] = "Column4";
        columns[3] = "Column3"; // duplicate column
        criteria.addColumnsToGet(columns);
        assertEquals(criteria.numColumnsToGet(), 4);
        assertTrue(criteria.getColumnsToGet().contains("Column1"));
        assertTrue(criteria.getColumnsToGet().contains("Column2"));
        assertTrue(criteria.getColumnsToGet().contains("Column3"));
        assertTrue(criteria.getColumnsToGet().contains("Column4"));

        criteria.addColumnsToGet("Column2"); // duplicate column
        criteria.addColumnsToGet("Column3"); // duplicate column
        assertEquals(criteria.numColumnsToGet(), 4);
        assertTrue(criteria.getColumnsToGet().contains("Column1"));
        assertTrue(criteria.getColumnsToGet().contains("Column2"));
        assertTrue(criteria.getColumnsToGet().contains("Column3"));
        assertTrue(criteria.getColumnsToGet().contains("Column4"));

        List<String> columnsToAdd = new ArrayList<String>();
        columnsToAdd.add("Column5");
        columnsToAdd.add("Column6");
        columnsToAdd.add("Column6"); // duplicate column
        columnsToAdd.add("Column7");
        criteria.addColumnsToGet(columnsToAdd);
        assertEquals(criteria.numColumnsToGet(), 7);
        assertTrue(criteria.getColumnsToGet().contains("Column1"));
        assertTrue(criteria.getColumnsToGet().contains("Column2"));
        assertTrue(criteria.getColumnsToGet().contains("Column3"));
        assertTrue(criteria.getColumnsToGet().contains("Column4"));
        assertTrue(criteria.getColumnsToGet().contains("Column5"));
        assertTrue(criteria.getColumnsToGet().contains("Column6"));
        assertTrue(criteria.getColumnsToGet().contains("Column7"));
    }
}
