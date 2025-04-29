package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSingleColumnValueFilter {

    @Test
    public void testOperations() {
        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("column", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false));
        assertEquals(filter.getColumnName(), "column");
        assertEquals(filter.getOperator(), SingleColumnValueFilter.CompareOperator.EQUAL);
        assertEquals(filter.getColumnValue(), ColumnValue.fromBoolean(false));
        assertEquals(filter.getFilterType(), FilterType.SINGLE_COLUMN_VALUE_FILTER);
        assertEquals(filter.isPassIfMissing(), true);
        assertEquals(filter.isLatestVersionsOnly(), true);

        filter.setColumnName("another_column");
        assertEquals(filter.getColumnName(), "another_column");

        filter.setColumnValue(ColumnValue.fromString("abcde"));
        assertEquals(filter.getColumnValue(), ColumnValue.fromString("abcde"));

        filter.setOperator(SingleColumnValueFilter.CompareOperator.GREATER_EQUAL);
        assertEquals(filter.getOperator(), SingleColumnValueFilter.CompareOperator.GREATER_EQUAL);

        filter.setPassIfMissing(true);
        assertEquals(filter.isPassIfMissing(), true);

        filter.setLatestVersionsOnly(false);
        assertEquals(filter.isLatestVersionsOnly(), false);
    }

    @Test
    public void testInvalidArguments() {
        try {
            new SingleColumnValueFilter("", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("a"));
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new SingleColumnValueFilter("c", null, ColumnValue.fromString("a"));
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new SingleColumnValueFilter("c", SingleColumnValueFilter.CompareOperator.EQUAL, null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        SingleColumnValueFilter filter =
                new SingleColumnValueFilter("column", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromBoolean(false));
        try {
            filter.setColumnName("");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            filter.setColumnName(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            filter.setColumnValue(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            filter.setOperator(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
