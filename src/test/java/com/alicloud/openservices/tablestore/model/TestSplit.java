package com.alicloud.openservices.tablestore.model;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestSplit {
    
    private int maxNumberPrimaryKeyColumnEachRow = 4;

    @Test
    public void testConstructor() {
        Split s = new Split();
        assertEquals(s.getLocation(), null);
        assertEquals(s.getLowerBound(), null);
        assertEquals(s.getUpperBound(), null);
        
        String lowerKeyNamePrefix = "lowerKeyNamePrefix";
        String upperKeyNamePrefix = "upperKeyNamePrefix";
        String lowerKeyValuePrefix = "lowerKeyValuePrefix";
        String upperKeyValuePrefix = "upperKeyValuePrefix";
        List<PrimaryKeyColumn> lpkcl = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upkcl = new ArrayList<PrimaryKeyColumn>();
        for ( int i = 0; i < maxNumberPrimaryKeyColumnEachRow; ++i ) {
            lpkcl.add(new PrimaryKeyColumn(lowerKeyNamePrefix + i, PrimaryKeyValue.fromString(lowerKeyValuePrefix + i)));
            upkcl.add(new PrimaryKeyColumn(upperKeyNamePrefix + i, PrimaryKeyValue.fromString(upperKeyValuePrefix + i)));
        }
        PrimaryKey lpk = new PrimaryKey(lpkcl);
        PrimaryKey upk = new PrimaryKey(upkcl);
        String loc = "location";
        s = new Split(loc, lpk, upk);
        assertTrue(s.getLocation().equals(loc));
        assertTrue(s.getLowerBound().equals(lpk));
        assertTrue(s.getUpperBound().equals(upk));
    }
    
    @Test
    public void testSetterAndGetter() {
        String lowerKeyNamePrefix = "lowerKeyNamePrefix";
        String upperKeyNamePrefix = "upperKeyNamePrefix";
        String lowerKeyValuePrefix = "lowerKeyValuePrefix";
        String upperKeyValuePrefix = "upperKeyValuePrefix";
        List<PrimaryKeyColumn> lpkcl = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> upkcl = new ArrayList<PrimaryKeyColumn>();
        for ( int i = 0; i < maxNumberPrimaryKeyColumnEachRow; ++i ) {
            lpkcl.add(new PrimaryKeyColumn(lowerKeyNamePrefix + i, PrimaryKeyValue.fromString(lowerKeyValuePrefix + i)));
            upkcl.add(new PrimaryKeyColumn(upperKeyNamePrefix + i, PrimaryKeyValue.fromString(upperKeyValuePrefix + i)));
        }
        PrimaryKey lpk = new PrimaryKey(lpkcl);
        PrimaryKey upk = new PrimaryKey(upkcl);
        String loc = "location";
        Split s = new Split();
        s.setLocation(loc);
        s.setLowerBound(lpk);
        s.setUpperBound(upk);
        assertTrue(s.getLocation().equals(loc));
        assertTrue(s.getLowerBound().equals(lpk));
        assertTrue(s.getUpperBound().equals(upk));
    }
    
}
