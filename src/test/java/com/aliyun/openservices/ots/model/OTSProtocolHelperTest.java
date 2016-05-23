package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import org.junit.Test;

public class OTSProtocolHelperTest {
    
    @Test
    public void testPrimaryKeyTypeConvert() {
        assertEquals(OTSProtocolHelper.toPBColumnType(PrimaryKeyType.INTEGER),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(OTSProtocolHelper.toPBColumnType(PrimaryKeyType.STRING),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER),
                PrimaryKeyType.INTEGER);
        assertEquals(OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING),
                PrimaryKeyType.STRING);
        assertEquals(OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY),
                PrimaryKeyType.BINARY);
        try {
            OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE);
            fail("expect exception.");
        } catch(Exception e){
        }
        try {
            OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN);
            fail("expect exception.");
        } catch(Exception e){
        }
        try {
            OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MIN);
            fail("expect exception.");
        } catch(Exception e){
        }
        try {
            OTSProtocolHelper.toPrimaryKeyType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MAX);
            fail("expect exception.");
        } catch(Exception e){
        }
    }
    
    @Test
    public void testColumnTypeConvert() {
        assertEquals(OTSProtocolHelper.toPBColumnType(ColumnType.BOOLEAN),
            com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN);
        assertEquals(OTSProtocolHelper.toPBColumnType(ColumnType.INTEGER),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(OTSProtocolHelper.toPBColumnType(ColumnType.STRING),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(OTSProtocolHelper.toPBColumnType(ColumnType.DOUBLE),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE);
        assertEquals(OTSProtocolHelper.toPBColumnType(ColumnType.BINARY),
                com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY);
        assertEquals(OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN),
                ColumnType.BOOLEAN);
        assertEquals(OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER),
                ColumnType.INTEGER);
        assertEquals(OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING),
                ColumnType.STRING);
        assertEquals(OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE),
                ColumnType.DOUBLE);
        assertEquals(OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY),
                ColumnType.BINARY);
        
        try {
            OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MIN);
            fail("expect exception.");
        } catch(Exception e){
        }
        try {
            OTSProtocolHelper.toColumnType(com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INF_MAX);
            fail("expect exception.");
        } catch(Exception e){
        }
    }
    
    @Test
    public void testPrimaryKeyValueConvert() {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue pbValue = 
                OTSProtocolHelper.buildColumnValue(PrimaryKeyValue.fromLong(Integer.MAX_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(pbValue.getVInt(), Integer.MAX_VALUE);
        
        PrimaryKeyValue value = OTSProtocolHelper.toPrimaryKeyValue(pbValue);
        assertEquals(value.asLong(), Integer.MAX_VALUE);
        
        pbValue = OTSProtocolHelper.buildColumnValue(PrimaryKeyValue.fromLong(Integer.MIN_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(pbValue.getVInt(), Integer.MIN_VALUE);
        
        value = OTSProtocolHelper.toPrimaryKeyValue(pbValue);
        assertEquals(value.asLong(), Integer.MIN_VALUE);
        
        pbValue = OTSProtocolHelper.buildColumnValue(PrimaryKeyValue.fromString(""));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(pbValue.getVString(), "");
        
        value = OTSProtocolHelper.toPrimaryKeyValue(pbValue);
        assertEquals(value.asString(), "");
        
        pbValue = OTSProtocolHelper.buildColumnValue(PrimaryKeyValue.fromString("阿里云"));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(pbValue.getVString(), "阿里云");
        
        value = OTSProtocolHelper.toPrimaryKeyValue(pbValue);
        assertEquals(value.asString(), "阿里云");
    }
    
    @Test
    public void testColumnValueConvert() {
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnValue pbValue = 
                OTSProtocolHelper.buildColumnValue(ColumnValue.fromLong(Integer.MAX_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(pbValue.getVInt(), Integer.MAX_VALUE);
        
        ColumnValue value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asLong(), Integer.MAX_VALUE);
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromLong(Integer.MIN_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.INTEGER);
        assertEquals(pbValue.getVInt(), Integer.MIN_VALUE);
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asLong(), Integer.MIN_VALUE);
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromString(""));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(pbValue.getVString(), "");
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asString(), "");
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromString("阿里云"));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.STRING);
        assertEquals(pbValue.getVString(), "阿里云");
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asString(), "阿里云");
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromBoolean(true));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN);
        assertEquals(pbValue.getVBool(), true);
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asBoolean(), true);
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromBoolean(false));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BOOLEAN);
        assertEquals(pbValue.getVBool(), false);
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(value.asBoolean(), false);
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromDouble(Double.MAX_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE);
        assertEquals(Double.toHexString(pbValue.getVDouble()), Double.toHexString(Double.MAX_VALUE));
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(Double.toHexString(value.asDouble()), Double.toHexString(Double.MAX_VALUE));
        
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromDouble(Double.MIN_VALUE));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.DOUBLE);
        assertEquals(Double.toHexString(pbValue.getVDouble()), Double.toHexString(Double.MIN_VALUE));
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertEquals(Double.toHexString(value.asDouble()), Double.toHexString(Double.MIN_VALUE));
        
        byte[] bs = {10, 11, 12, 13, 14, 15, 127, -1, -128};
        pbValue = OTSProtocolHelper.buildColumnValue(ColumnValue.fromBinary(bs));
        assertEquals(pbValue.getType(), com.aliyun.openservices.ots.protocol.OtsProtocol2.ColumnType.BINARY);
        assertArrayEquals(bs, pbValue.getVBinary().toByteArray());
        
        value = OTSProtocolHelper.toColumnValue(pbValue);
        assertArrayEquals(bs, value.asBinary());
    }
    
    @Test
    public void testRowExistenceExpectationConvertor() {
        assertEquals(OTSProtocolHelper.toPBRowExistenceExpectation(RowExistenceExpectation.EXPECT_EXIST), 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.EXPECT_EXIST);
        
        assertEquals(OTSProtocolHelper.toPBRowExistenceExpectation(RowExistenceExpectation.EXPECT_NOT_EXIST), 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.EXPECT_NOT_EXIST);
        
        assertEquals(OTSProtocolHelper.toPBRowExistenceExpectation(RowExistenceExpectation.IGNORE), 
                com.aliyun.openservices.ots.protocol.OtsProtocol2.RowExistenceExpectation.IGNORE);
    }
    
    @Test
    public void testDirectionConvertor() {
        assertEquals(OTSProtocolHelper.toPBDirection(Direction.BACKWARD), com.aliyun.openservices.ots.protocol.OtsProtocol2.Direction.BACKWARD);
        assertEquals(OTSProtocolHelper.toPBDirection(Direction.FORWARD), com.aliyun.openservices.ots.protocol.OtsProtocol2.Direction.FORWARD);
    }
}
