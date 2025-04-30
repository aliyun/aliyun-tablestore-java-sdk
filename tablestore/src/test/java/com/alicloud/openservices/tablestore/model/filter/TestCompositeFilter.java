package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCompositeFilter {

    @Test
    public void testOperations() {
        // col1 > 0 and (col2 = 1 or col3 < 2)
        CompositeColumnValueFilter subFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
        subFilter.addFilter(new SingleColumnValueFilter("col2", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(1)))
                .addFilter(new SingleColumnValueFilter("col3", SingleColumnValueFilter.CompareOperator.LESS_THAN, ColumnValue.fromLong(2)));

        CompositeColumnValueFilter filter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.AND);
        filter.addFilter(new SingleColumnValueFilter("col1", SingleColumnValueFilter.CompareOperator.GREATER_THAN, ColumnValue.fromLong(0)))
                .addFilter(subFilter);

        assertEquals(filter.getOperationType(), CompositeColumnValueFilter.LogicOperator.AND);
        assertEquals(filter.getFilterType(), FilterType.COMPOSITE_COLUMN_VALUE_FILTER);
        assertEquals(filter.getSubFilters().size(), 2);
    }

    @Test
    public void testInvalidArguments() {
        CompositeColumnValueFilter subFilter = new CompositeColumnValueFilter(CompositeColumnValueFilter.LogicOperator.OR);
        try {
            subFilter.addFilter(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
