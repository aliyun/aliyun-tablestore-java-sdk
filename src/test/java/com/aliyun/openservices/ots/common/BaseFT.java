package com.aliyun.openservices.ots.common;


import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BaseFT {

    private static final Logger LOG = LoggerFactory.getLogger(BaseFT.class);

    public static void assertOTSException(
            OTSException expect,
            OTSException actual) {
        LOG.info(actual.toString());
        assertEquals(expect.getErrorCode(), actual.getErrorCode());
        assertEquals(expect.getMessage(), actual.getMessage());
        assertEquals(expect.getHttpStatus(), actual.getHttpStatus());
    }

    public static void assertOTSException(
            String errorCode,
            String errorMsg,
            int httpCode,
            OTSException actual) {
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

    public static void assertClientException(
            String errorMsg,
            ClientException actual) {
        LOG.info(actual.toString());
        assertEquals(errorMsg, actual.getMessage());
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

        checkPrimaryKeySchemas(expect.getPrimaryKey(), actual.getPrimaryKey());
    }

    public static void checkDescribeTableResult(
            DescribeTableResult expect,
            DescribeTableResult actual) {
        checkTableMeta(expect.getTableMeta(), actual.getTableMeta());
    }

    public static void checkColumns(
            Map<String, ColumnValue> expect,
            Map<String, ColumnValue> actual) {
        assertEquals(expect.size(), actual.size());
        for (Map.Entry<String, ColumnValue> entry : expect.entrySet()) {
            ColumnValue expectValue = entry.getValue();
            ColumnValue actualValue = actual.get(entry.getKey());
            assertEquals(expectValue, actualValue);
        }
    }

    public static void checkRow(
            Row expect,
            Row actual) {
        checkColumns(expect.getColumns(), actual.getColumns());
    }

}
