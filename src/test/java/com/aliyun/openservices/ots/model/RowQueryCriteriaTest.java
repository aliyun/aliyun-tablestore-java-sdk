package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.SingleRowQueryCriteria;

public class RowQueryCriteriaTest {
    private String tableName = "valid_table";

    @Test
    public void testSingleRowQueryCriteria() {
        // valid table name
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        assertEquals(tableName, criteria.getTableName());
        assertTrue(criteria.getRowPrimaryKey() != null);
        assertTrue(criteria.getColumnsToGet() != null);
        criteria = new SingleRowQueryCriteria(tableName);
        assertEquals(tableName, criteria.getTableName());

        // with PK
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(1));
        criteria.setPrimaryKey(pks);
        criteria.addColumnsToGet(new String[] { "name" });
        assertEquals(1, criteria.getRowPrimaryKey().getPrimaryKey().get("uid").asLong());
        assertEquals("name", criteria.getColumnsToGet().get(0));
    }

    @Test
    public void testRangeRowQueryCriteria(){
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
        RowPrimaryKey inclusiveStartPrimaryKey = new RowPrimaryKey();
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(1));
        inclusiveStartPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MIN);
        
        RowPrimaryKey exclusiveEndPrimaryKey = new RowPrimaryKey();
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("uid", PrimaryKeyValue.fromLong(1));
        exclusiveEndPrimaryKey.addPrimaryKeyColumn("name", PrimaryKeyValue.INF_MIN);
        
        criteria.setInclusiveStartPrimaryKey(inclusiveStartPrimaryKey);
        criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);

        criteria.addColumnsToGet("col1");
        criteria.addColumnsToGet("col2");
        
        criteria.setLimit(100);

        assertTrue(criteria.getInclusiveStartPrimaryKey() != null);
        assertTrue(criteria.getExclusiveEndPrimaryKey() != null);

        assertEquals(1, criteria.getInclusiveStartPrimaryKey().getPrimaryKey().get("uid").asLong());
        assertEquals(1, criteria.getExclusiveEndPrimaryKey().getPrimaryKey().get("uid").asLong());
        
        assertEquals("col1", criteria.getColumnsToGet().get(0));
        assertEquals("col2", criteria.getColumnsToGet().get(1));
        
        assertEquals(100, criteria.getLimit());
        
        try{
            criteria.setLimit(-1);
        }
        catch(ArithmeticException e) {}
    }
}
