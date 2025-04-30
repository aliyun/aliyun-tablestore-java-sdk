package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;

public class Utils {
    
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static void sleepSeconds(int seconds) {
       sleepMillis(seconds * 1000L);
    }

    public static void sleepMillis(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static boolean checkNameExiste(List<String> names, String name) {
        for (String n : names) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }
    
    public static PrimaryKeyValue getPKColumnValue(PrimaryKeyType type, String value) throws UnsupportedEncodingException {
        switch (type) {
            case INTEGER:
                return PrimaryKeyValue.fromLong(Long.valueOf(value));
            case STRING:
                return PrimaryKeyValue.fromString(value);
            case BINARY:
                return PrimaryKeyValue.fromBinary(value.getBytes("UTF-8"));
            default:
                throw new RuntimeException("Bug: not support : " + type);
        }
    }
    
    public static PrimaryKey getMinPK(List<PrimaryKeySchema> scheme) {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeySchema s : scheme) {
            builder.addPrimaryKeyColumn(s.getName(), PrimaryKeyValue.INF_MIN);
        }
        return builder.build();
    }
    
    public static PrimaryKey getMaxPK(List<PrimaryKeySchema> scheme) {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeySchema s : scheme) {
            builder.addPrimaryKeyColumn(s.getName(), PrimaryKeyValue.INF_MAX);
        }
        return builder.build();
    }
    
    
    public static void deleteTableIfExist(SyncClientInterface ots, String tableName) {
        try {
            OTSHelper.deleteTable(ots, tableName);
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw e;
            }
        }
    }

    public static SyncClientInterface getOTSInstance() {
        return getOTSInstance(null);
    }


    public static TimeseriesClient getTsClient() {
        ServiceSettings serviceSettings = ServiceSettings.load();
        return new TimeseriesClient(
                serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(),
                serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName()
        );
    }


    public static SyncClientInterface getOTSInstance(ClientConfiguration configuration) {
        ServiceSettings serviceSettings = ServiceSettings.load();
        return new SyncClient(
                serviceSettings.getOTSEndpoint(),
                serviceSettings.getOTSAccessKeyId(),
                serviceSettings.getOTSAccessKeySecret(),
                serviceSettings.getOTSInstanceName(),
                configuration
        );
    }

    public static void checkColumns(Column[] columns, List<Column> expect) {
        checkColumns(columns, expect, true);
    }
    
    public static void checkColumns(Column[] columns, List<Column> expect,
                                    boolean isCheckTimestamp) {
        //LOG.info("Expect Size: {}, actual size : {}, isCheckTimestamp : {} ", expect.size(), columns.length, isCheckTimestamp);
        LOG.info("Expect:");
        for (Column c : expect) {
            LOG.info(c.toString());
        }
        LOG.info("Actual:");
        for (Column c : columns) {
            LOG.info(c.toString());
        }
        assertEquals(expect.size(), columns.length);
        for (int i = 0; i < columns.length; i++) {
            LOG.info("expect:{}, actual:{}", expect.get(i).toString(), columns[i].toString());
            assertEquals(expect.get(i).getName(), columns[i].getName());
            assertEquals(expect.get(i).getValue(), columns[i].getValue());
            if (isCheckTimestamp == true) {
                assertEquals(expect.get(i).getTimestamp(), columns[i].getTimestamp());
            }
        }
    }

    public static void waitForPartitionLoad(String tableName) {
        sleepSeconds(OTSTestConst.CREATE_TABLE_SLEEP_IN_SECOND);
    }

    public static void waitForConditionWithRetry(long waitIntervalInMillis, long timeoutInMillis, BooleanSupplier condition) {
        long start = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - start > timeoutInMillis) {
                throw new RuntimeException("waitForConditionWithRetry timeout");
            }
            if (condition.getAsBoolean()) {
                break;
            }
            sleepMillis(waitIntervalInMillis);
        }
    }
}

