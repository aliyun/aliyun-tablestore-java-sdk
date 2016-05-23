package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowDeleteChange;
import com.aliyun.openservices.ots.model.RowPutChange;

public class RowChangeTest {

    @Test
    public void testRowPutChange() {
        RowPutChange rc = new RowPutChange("tableName");
        assertTrue(rc.getRowPrimaryKey() != null);
        assertTrue(rc.getAttributeColumns() != null);
        
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        rc.setPrimaryKey(pks);
        rc.addAttributeColumn("col1", ColumnValue.fromDouble(3.5));
        rc.addAttributeColumn("col2", ColumnValue.fromLong(5));
        
        assertEquals(1, rc.getRowPrimaryKey().getPrimaryKey().get("pk1").asLong());
        assertEquals("", 3.5, rc.getAttributeColumns().get("col1").asDouble(), 0);
        assertEquals(5, rc.getAttributeColumns().get("col2").asLong());
    }

    @Test
    public void testRowDeleteChange() {
        RowDeleteChange rc = new RowDeleteChange("tableName");
        assertTrue(rc.getRowPrimaryKey() != null);
        
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        rc.setPrimaryKey(pks);
        assertEquals(1, rc.getRowPrimaryKey().getPrimaryKey().get("pk1").asLong());
    }
    
    @Test
    public void testRowUpdateChange() {
        RowUpdateChange rc = new RowUpdateChange("tableName");
        RowPrimaryKey pks = new RowPrimaryKey();
        pks.addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(1));
        rc.setPrimaryKey(pks);
        
        rc.addAttributeColumn("col1", ColumnValue.fromLong(2));
        assertEquals(1, rc.getRowPrimaryKey().getPrimaryKey().size());
        assertEquals(rc.getRowPrimaryKey().getPrimaryKey().get("pk1").asLong(), 1);
        assertEquals(1, rc.getAttributeColumns().size());
        assertEquals(rc.getAttributeColumns().get("col1").asLong(), 2);
        
        rc.deleteAttributeColumn("col1");
        assertEquals(1, rc.getAttributeColumns().size());
        assertTrue(rc.getAttributeColumns().get("col1") == null);
    }
}