package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSingleRowQueryCriteria {

    @Test
    public void testConstructor() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria("T", primaryKey);

        assertEquals(criteria.getTableName(), "T");
        assertEquals(criteria.getPrimaryKey(), primaryKey);

        assertTrue(criteria.compareTo(criteria) == 0);
    }

    @Test
    public void testInvalidArguments() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("PK0", PrimaryKeyValue.fromLong(1912)).build();
        try {
            new SingleRowQueryCriteria("", primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new SingleRowQueryCriteria(null, primaryKey);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new SingleRowQueryCriteria("T", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new SingleRowQueryCriteria("T", PrimaryKeyBuilder.createPrimaryKeyBuilder().build());
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
