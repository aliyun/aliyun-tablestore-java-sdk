package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueRegexFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ColumnNullValueValidationTest {

    @Test
    public void testRowPutChangeIncludesColumnName() {
        RowPutChange rowChange = new RowPutChange("T");
        assertNullValueMessage("ColumnWithoutTimestamp", () ->
                rowChange.addColumn("ColumnWithoutTimestamp", null));
        assertNullValueMessage("ColumnWithTimestamp", () ->
                rowChange.addColumn("ColumnWithTimestamp", null, 1418630000));
    }

    @Test
    public void testSingleColumnValueFilterIncludesColumnName() {
        assertNullValueMessage("FilterColumn", () ->
                new SingleColumnValueFilter(
                        "FilterColumn",
                        SingleColumnValueFilter.CompareOperator.EQUAL,
                        null));
    }

    @Test
    public void testSingleColumnValueRegexFilterIncludesColumnName() {
        assertNullValueMessage("RegexColumn", () ->
                new SingleColumnValueRegexFilter(
                        "RegexColumn",
                        SingleColumnValueRegexFilter.CompareOperator.EQUAL,
                        null));
    }

    @Test
    public void testSingleColumnValueConditionIncludesColumnName() {
        assertNullValueMessage("ConditionColumn", () ->
                new SingleColumnValueCondition(
                        "ConditionColumn",
                        SingleColumnValueCondition.CompareOperator.EQUAL,
                        null));
    }

    private void assertNullValueMessage(String columnName, Runnable action) {
        try {
            action.run();
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(
                    "The value of column '" + columnName + "' should not be null.",
                    e.getMessage());
        }
    }
}
