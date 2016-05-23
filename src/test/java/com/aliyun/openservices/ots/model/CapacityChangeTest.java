package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import org.junit.Test;

public class CapacityChangeTest {
    
    @Test
    public void testCapacityChange() {
        ReservedThroughputChange cc = new ReservedThroughputChange();
        
        assertEquals(cc.isReadSet(), false);
        assertEquals(cc.isWriteSet(), false);
        
        assertEquals(cc.getReadCapacityUnit(), 0);
        assertEquals(cc.getWriteCapacityUnit(), 0);
        
        cc.setReadCapacityUnit(10);
        assertEquals(cc.isReadSet(), true);
        assertEquals(cc.isWriteSet(), false);
        
        assertEquals(cc.getReadCapacityUnit(), 10);
        assertEquals(cc.getWriteCapacityUnit(), 0);
        
        cc.setWriteCapacityUnit(11);
        assertEquals(cc.isReadSet(), true);
        assertEquals(cc.isWriteSet(), true);
        
        assertEquals(cc.getReadCapacityUnit(), 10);
        assertEquals(cc.getWriteCapacityUnit(), 11);
        
        cc.clearReadCapacityUnit();
        cc.clearWriteCapacityUnit();
        assertEquals(cc.isReadSet(), false);
        assertEquals(cc.isWriteSet(), false);
    }

}
