package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TestMultiRowQueryCriteria {

    @Test
    public void testEmpty() {
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("T");

        assertEquals(criteria.getTableName(), "T");
        assertTrue(criteria.getRowKeys().isEmpty());
        assertTrue(criteria.get(0) == null);
        assertEquals(criteria.size(), 0);
        assertTrue(criteria.isEmpty());
    }

    @Test
    public void testAddPrimaryKey() {
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("T");

        PrimaryKey pk = TestUtil.randomPrimaryKey(TestUtil.randomPrimaryKeySchema(5));
        criteria.addRow(pk);

        assertEquals(criteria.size(), 1);
        assertEquals(criteria.getRowKeys().size(), 1);
        assertEquals(criteria.get(0), pk);
        assertEquals(criteria.getRowKeys().get(0), pk);
        assertTrue(!criteria.isEmpty());

        PrimaryKey[] pks = new PrimaryKey[10];
        PrimaryKeySchema[] pkSchema = TestUtil.randomPrimaryKeySchema(10);
        for (int i = 0; i < pks.length; i++) {
            pks[i] = TestUtil.randomPrimaryKey(pkSchema);
        }

        criteria.setRowKeys(Arrays.asList(pks));
        assertEquals(criteria.size(), pks.length);
        assertArrayEquals(criteria.getRowKeys().toArray(), pks);
        for (int i = 0; i < pks.length; i++) {
            assertEquals(criteria.get(i), pks[i]);
        }

        assertTrue(!criteria.isEmpty());

        criteria.clear();
        assertTrue(criteria.getRowKeys().isEmpty());
        assertTrue(criteria.get(0) == null);
        assertEquals(criteria.size(), 0);
        assertTrue(criteria.isEmpty());
    }

    @Test
    public void testInvalidArguments() {
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("T");
        try {
            criteria.addRow(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            criteria.setRowKeys(null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            criteria.setRowKeys(new ArrayList<PrimaryKey>());
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testClone() {
        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria("T");
        PrimaryKey[] pks = new PrimaryKey[10];
        PrimaryKeySchema[] pkSchema = TestUtil.randomPrimaryKeySchema(10);
        for (int i = 0; i < pks.length; i++) {
            pks[i] = TestUtil.randomPrimaryKey(pkSchema);
        }

        criteria.setRowKeys(Arrays.asList(pks));
        assertEquals(criteria.size(), pks.length);
        assertArrayEquals(criteria.getRowKeys().toArray(), pks);

        criteria.setTimestamp(1912);
        Filter filter = new SingleColumnValueFilter("column",
                SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromLong(10));
        criteria.setFilter(filter);

        MultiRowQueryCriteria newCriteria = criteria.cloneWithoutRowKeys();
        assertTrue(newCriteria != criteria);
        assertTrue(newCriteria.getRowKeys().isEmpty());
        assertTrue(newCriteria.get(0) == null);
        assertEquals(newCriteria.size(), 0);
        assertTrue(newCriteria.isEmpty());
        assertTrue(newCriteria.getFilter() != null);

        assertTrue(newCriteria.hasSetTimeRange());
        assertTrue(!newCriteria.hasSetMaxVersions());
        assertEquals(newCriteria.getTimeRange(), criteria.getTimeRange());

        Filter newFilter = newCriteria.getFilter();
        assertTrue(newFilter instanceof SingleColumnValueFilter);
        assertEquals(((SingleColumnValueFilter)newFilter).getOperator(), SingleColumnValueFilter.CompareOperator.EQUAL);
        assertEquals(((SingleColumnValueFilter)newFilter).getColumnValue(), ColumnValue.fromLong(10));
        assertEquals(((SingleColumnValueFilter)newFilter).getColumnName(), "column");
    }
}
