package com.alicloud.openservices.tablestore.common;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BaseFT {

    private static final Logger LOG = LoggerFactory.getLogger(BaseFT.class);

    public static void assertTableStoreException(
            TableStoreException expect,
            TableStoreException actual) {
        LOG.info(actual.toString());
        assertEquals(expect.getErrorCode(), actual.getErrorCode());
        assertEquals(expect.getMessage(), actual.getMessage());
        assertEquals(expect.getHttpStatus(), actual.getHttpStatus());
    }

    public static void assertTableStoreException(
            String errorCode,
            String errorMsg,
            int httpCode,
            TableStoreException actual) {
        LOG.info(actual.toString());
        assertEquals(errorCode, actual.getErrorCode());
        assertEquals(errorMsg, actual.getMessage());
        assertEquals(httpCode, actual.getHttpStatus());
    }

    public static void assertClientException(
            ClientException expect,
            ClientException actual) {
        LOG.info(actual.toString());
        assertEquals(expect.getMessage(), actual.getMessage());
    }

    public static void checkTimestampWithDeviation(long expect, long actual) {
        LOG.info("Expect Ts: {}, Actual Ts: {}", expect, actual);
        long begin = expect - (OTSTestConst.TIMESTAMP_DEVIATION_IN_SECOND * 1000);
        long end = expect + (OTSTestConst.TIMESTAMP_DEVIATION_IN_SECOND * 1000);
        if (!(begin < actual && actual < end)) {
            fail(String.format("Expect:%d, Actual: %d", expect, actual));
        }
    }

    public static void checkPrimaryKeySchemas(
            Map<String, PrimaryKeyType> expectPK,
            Map<String, PrimaryKeyType> actualPK ) {
        assertEquals(expectPK.size(), actualPK.size());

        for (int i = 0; i < expectPK.size(); i++) {
            assertEquals(expectPK.get(i), actualPK.get(i));
        }
    }
    
    public static void checkTableMeta(
            TableMeta expect,
            TableMeta actual) {
        assertEquals(expect.getTableName(), actual.getTableName());

        checkPrimaryKeySchemas(expect.getPrimaryKeyMap(), actual.getPrimaryKeyMap());
    }

    public static void checkDescribeTableResult(
            DescribeTableResponse expect,
            DescribeTableResponse actual) {
        checkTableMeta(expect.getTableMeta(), actual.getTableMeta());
    }
    
    public static void checkColumns(
            Column[] expect,
            Column[] actual) {
        assertEquals(expect.length, actual.length);
        for (int i = 0; i < expect.length; i++) {
            assertEquals(expect[i], actual[i]);
        }
    }

    public static void checkColumns(
    		NavigableMap<String, NavigableMap<Long, ColumnValue>> expect,
    		NavigableMap<String, NavigableMap<Long, ColumnValue>> actual) {
        assertEquals(expect.size(), actual.size());
        for (Map.Entry<String, NavigableMap<Long, ColumnValue>> entry : expect.entrySet()) {
        	NavigableMap<Long, ColumnValue> expectValueList = entry.getValue();
        	NavigableMap<Long, ColumnValue> actualValueList = actual.get(entry.getKey());
            assertEquals(expectValueList.size(), actualValueList.size());
            for (Long ts : expectValueList.keySet()) {
                assertEquals(expectValueList.get(ts), actualValueList.get(ts));
            }
        }
    }
    
    public static void checkColumnsNoTimestamp(
            Column[] expect,
            Column[] actual) {
        assertEquals(expect.length, actual.length);
        for (int i = 0; i < expect.length; i++) {
            assertEquals(expect[i].getName(), actual[i].getName());
            assertEquals(expect[i].getValue(), actual[i].getValue());
        }
    }

    public static void checkRow(
            Row expect,
            Row actual) {
    	if (expect == null) {
    		assertEquals(actual, null);
    	} else {
    		checkColumns(expect.getColumnsMap(), actual.getColumnsMap());
    	}
    }
    
    public static void checkRowNoTimestamp(
            Row expect,
            Row actual) {
        assertEquals(expect.getPrimaryKey(), actual.getPrimaryKey());
        
        checkColumnsNoTimestamp(expect.getColumns(), actual.getColumns());
    }
}
