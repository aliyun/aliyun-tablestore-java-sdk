package com.alicloud.openservices.tablestore.model;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestComputeSplitsBySizeRequest {

    @Test
    public void testConstructor() {
        String tableName = "testTableName";
        long splitSize = 5;
        
        ComputeSplitsBySizeRequest req = new ComputeSplitsBySizeRequest( tableName, splitSize );
        
        assertEquals( req.getTableName(), tableName );
        assertEquals( req.getSplitSizeIn100MB(), splitSize );
    }
    
    @Test
    public void testConstructorWithEmptyParameter() {
        ComputeSplitsBySizeRequest req = new ComputeSplitsBySizeRequest();
        
        assertEquals( req.getTableName(), null );
        assertEquals( req.getSplitSizeIn100MB(), 0l );
    }
    
    @Test
    public void testSetterAndGetter() {
        String tableName = "testTableName";
        long splitSize = 5;
        
        ComputeSplitsBySizeRequest req = new ComputeSplitsBySizeRequest();
        req.setTableName(tableName);
        req.setSplitSizeIn100MB(splitSize);
        
        assertEquals( req.getTableName(), tableName );
        assertEquals( req.getSplitSizeIn100MB(), splitSize );
    }
    
}
