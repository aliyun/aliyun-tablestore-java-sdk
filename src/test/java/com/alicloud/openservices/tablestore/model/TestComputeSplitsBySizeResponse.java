package com.alicloud.openservices.tablestore.model;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class TestComputeSplitsBySizeResponse {
    
    private int maxNumberPrimaryKeyColumnEachRow = 4;
    
    @Test
    public void testConstructorWithEmptyParameter() {
        ComputeSplitsBySizeResponse resp = new ComputeSplitsBySizeResponse(new Response());
        assertEquals(resp.getConsumedCapacity(), null);
        assertEquals(resp.getPrimaryKeySchema().size(), 0);
        assertEquals(resp.getSplits().size(), 0);
    }
    
    @Test
    public void testSetterAndGetterForComsumedCapacity() {
        ComputeSplitsBySizeResponse resp = new ComputeSplitsBySizeResponse(new Response());
        
        ConsumedCapacity cc = new ConsumedCapacity(new CapacityUnit(1, 1));
        resp.setConsumedCapacity(cc);
        assertTrue( resp.getConsumedCapacity().equals(cc) );
    }
    
    @Test
    public void testSetterAndGetterForSplitList() {
        ComputeSplitsBySizeResponse resp = new ComputeSplitsBySizeResponse(new Response());
        
        List<Split> sl1 = new ArrayList<Split>();
        resp.setSplits(sl1);
        assertEquals( resp.getSplits().size(), 0 );
        
        List<Split> sl2 = new ArrayList<Split>();
        
        String location = "location";
        String lowerPrimaryKeyNamePrefix = "lowerKeyNamePrefix_";
        String lowerPrimaryKeyValuePrefix = "lowerKeyValuePrefix_";
        String upperPrimaryKeyNamePrefix = "upperKeyNamePrefix_";
        String upperPrimaryKeyValuePrefix = "upperKeyValuePrefix_";
        List<PrimaryKeyColumn> pkcl1 = new ArrayList<PrimaryKeyColumn>();
        List<PrimaryKeyColumn> pkcl2 = new ArrayList<PrimaryKeyColumn>();
        for ( int i = 0; i < maxNumberPrimaryKeyColumnEachRow; ++i ) {
            pkcl1.add(new PrimaryKeyColumn( lowerPrimaryKeyNamePrefix + i, PrimaryKeyValue.fromString(lowerPrimaryKeyValuePrefix + i) ));
            pkcl2.add(new PrimaryKeyColumn( upperPrimaryKeyNamePrefix + i, PrimaryKeyValue.fromString(upperPrimaryKeyValuePrefix + i) ));
        }
        PrimaryKey pk1 = new PrimaryKey(pkcl1);
        PrimaryKey pk2 = new PrimaryKey(pkcl2);
        
        Split s = new Split(location, pk1, pk2);
        sl2.add(s);
        
        resp.addSplit(s);
        assertEquals( resp.getSplits().size(), 1 );
        assertTrue( resp.getSplits().get(0).getLocation().equals(location) );
        assertTrue( resp.getSplits().get(0).getLowerBound().equals(pk1) );
        assertTrue( resp.getSplits().get(0).getUpperBound().equals(pk2) );
        
        resp.setSplits(null);
        assertEquals( resp.getSplits(), null );
        
        resp.setSplits(sl2);
        assertEquals( resp.getSplits().size(), 1 );
        assertTrue( resp.getSplits().get(0).getLocation().equals(location) );
        assertTrue( resp.getSplits().get(0).getLowerBound().equals(pk1) );
        assertTrue( resp.getSplits().get(0).getUpperBound().equals(pk2) );
    }
    
    @Test
    public void testSetterAndGetterForPrimaryKeySchemaList() {
        ComputeSplitsBySizeResponse resp = new ComputeSplitsBySizeResponse(new Response());
        resp.setPrimaryKeySchema(null);
        assertEquals(resp.getPrimaryKeySchema(), null);
        
        List<PrimaryKeySchema> pksl = new ArrayList<PrimaryKeySchema>();
        resp.setPrimaryKeySchema(pksl);
        assertEquals(resp.getPrimaryKeySchema().size(), 0);
        
        List<PrimaryKeySchema> pksl2 = new ArrayList<PrimaryKeySchema>();
        String primaryKeySchemaPrefix = "primaryKeySchemaPrefix";
        for ( int j = 0; j < maxNumberPrimaryKeyColumnEachRow; ++j ) {
            pksl2.add(new PrimaryKeySchema(primaryKeySchemaPrefix + j, PrimaryKeyType.STRING));
        }
        resp.setPrimaryKeySchema(pksl2);
        assertEquals(resp.getPrimaryKeySchema().size(), maxNumberPrimaryKeyColumnEachRow);
        for ( int j = 0; j < maxNumberPrimaryKeyColumnEachRow; ++j ) {
            assertTrue( resp.getPrimaryKeySchema().get(j).getName().equals(primaryKeySchemaPrefix + j) );
            assertTrue( resp.getPrimaryKeySchema().get(j).getType().equals(PrimaryKeyType.STRING) );
        }
        
        resp = new ComputeSplitsBySizeResponse(new Response());
        String keyName = "name";
        resp.addPrimaryKeySchema(keyName, PrimaryKeyType.BINARY);
        assertEquals( resp.getPrimaryKeySchema().size(), 1 );
        assertTrue( resp.getPrimaryKeySchema().get(0).getName().equals(keyName) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getType().equals(PrimaryKeyType.BINARY) );
        
        resp = new ComputeSplitsBySizeResponse(new Response());
        String keyName2 = "name";
        resp.addPrimaryKeySchema(keyName2, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
        assertEquals( resp.getPrimaryKeySchema().size(), 1 );
        assertTrue( resp.getPrimaryKeySchema().get(0).getName().equals(keyName2) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getType().equals(PrimaryKeyType.INTEGER) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getOption().equals(PrimaryKeyOption.AUTO_INCREMENT) );
        
        resp = new ComputeSplitsBySizeResponse(new Response());
        String keyName3 = "name";
        PrimaryKeyType keyType3 = PrimaryKeyType.INTEGER;
        PrimaryKeyOption keyOption3 = PrimaryKeyOption.AUTO_INCREMENT;
        PrimaryKeySchema pks = new PrimaryKeySchema( keyName3, keyType3, keyOption3 );
        resp.addPrimaryKeySchema(pks);
        assertEquals( resp.getPrimaryKeySchema().size(), 1 );
        assertTrue( resp.getPrimaryKeySchema().get(0).getName().equals(keyName3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getType().equals(keyType3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getOption().equals(keyOption3) );
        
        String keyName4 = "name4";
        PrimaryKeyType keyType4 = PrimaryKeyType.INTEGER;
        PrimaryKeyOption keyOption4 = PrimaryKeyOption.AUTO_INCREMENT;
        PrimaryKeySchema pks2 = new PrimaryKeySchema( keyName4, keyType4, keyOption4 );
        List<PrimaryKeySchema> pksll = new ArrayList<PrimaryKeySchema>();
        pksll.add(pks2);
        resp.addPrimaryKeySchemas(pksll);
        assertEquals( resp.getPrimaryKeySchema().size(), 2 );
        assertTrue( resp.getPrimaryKeySchema().get(0).getName().equals(keyName3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getType().equals(keyType3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getOption().equals(keyOption3) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getName().equals(keyName4) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getType().equals(keyType4) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getOption().equals(keyOption4) );
        
        resp = new ComputeSplitsBySizeResponse(new Response());
        PrimaryKeySchema[] pkss = new PrimaryKeySchema[2];
        pkss[0] = pks;
        pkss[1] = pks2;
        resp.addPrimaryKeySchemas(pkss);
        assertTrue( resp.getPrimaryKeySchema().get(0).getName().equals(keyName3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getType().equals(keyType3) );
        assertTrue( resp.getPrimaryKeySchema().get(0).getOption().equals(keyOption3) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getName().equals(keyName4) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getType().equals(keyType4) );
        assertTrue( resp.getPrimaryKeySchema().get(1).getOption().equals(keyOption4) );
    }

}
