package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TimeseriesClient;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.TimeseriesMetaOptions;
import com.alicloud.openservices.tablestore.model.TimeseriesTableMeta;
import com.alicloud.openservices.tablestore.model.TimeseriesTableOptions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TimeseriesTableOpTest {

    static String testTable = "SDKTestTimeseriesTableOperation";

    static TimeseriesClient client = null;

    @BeforeClass
    public static void beforeClass() {
        ServiceSettings settings = ServiceSettings.load();
        final String endPoint = settings.getOTSEndpoint();
        final String accessId = settings.getOTSAccessKeyId();
        final String accessKey = settings.getOTSAccessKeySecret();
        final String instanceName = settings.getOTSInstanceName();

        client = new TimeseriesClient(endPoint, accessId, accessKey, instanceName);
    }

    @AfterClass
    public static void afterClass() {
        client.shutdown();
    }

    public static void deleteTable(String tableName) {
        try {
            client.deleteTimeseriesTable(new DeleteTimeseriesTableRequest(tableName));
        } catch (TableStoreException ex) {
            if (!ex.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                throw ex;
            }
        }
    }

    private void createTableWithTTL(Integer dataTTL, Integer metaTTL, Boolean allowUpdateAttributes) {
        TimeseriesTableMeta timeseriesTableMeta = new TimeseriesTableMeta(testTable);
        if (dataTTL != null) {
            TimeseriesTableOptions tableOptions = new TimeseriesTableOptions(dataTTL);
            timeseriesTableMeta.setTimeseriesTableOptions(tableOptions);
        }
        if (metaTTL != null || allowUpdateAttributes != null) {
            TimeseriesMetaOptions metaOptions = new TimeseriesMetaOptions();
            if (metaTTL != null) {
                metaOptions.setMetaTimeToLive(metaTTL);
            }
            if (allowUpdateAttributes != null) {
                metaOptions.setAllowUpdateAttributes(allowUpdateAttributes);
            }
            timeseriesTableMeta.setTimeseriesMetaOptions(metaOptions);
        }
        CreateTimeseriesTableRequest createTimeseriesTableRequest = new CreateTimeseriesTableRequest(timeseriesTableMeta);
        client.createTimeseriesTable(createTimeseriesTableRequest);
    }

    private void updateTableWithTTL(Integer dataTTL, Integer metaTTL, Boolean allowUpdateAttributes) {
        UpdateTimeseriesTableRequest updateTimeseriesTableRequest = new UpdateTimeseriesTableRequest(testTable);
        if (dataTTL != null) {
            updateTimeseriesTableRequest.setTimeseriesTableOptions(new TimeseriesTableOptions(dataTTL));
        }
        if (metaTTL != null || allowUpdateAttributes != null) {
            TimeseriesMetaOptions metaOptions = new TimeseriesMetaOptions();
            if (metaTTL != null) {
                metaOptions.setMetaTimeToLive(metaTTL);
            }
            if (allowUpdateAttributes != null) {
                metaOptions.setAllowUpdateAttributes(allowUpdateAttributes);
            }
            updateTimeseriesTableRequest.setTimeseriesMetaOptions(metaOptions);
        }
        client.updateTimeseriesTable(updateTimeseriesTableRequest);
    }

    private void assertCreateTableWithException(Integer dataTTL, Integer metaTTL, Boolean allowUpdateAttributes, String errorCode) {
        try {
            createTableWithTTL(dataTTL, metaTTL, allowUpdateAttributes);
            fail();
        } catch (TableStoreException exception) {
            assertEquals(errorCode, exception.getErrorCode());
        }
    }

    private void assertUpdateTableWithException(Integer dataTTL, Integer metaTTL, Boolean allowUpdateAttributes, String errorCode) {
        try {
            updateTableWithTTL(dataTTL, metaTTL, allowUpdateAttributes);
            fail();
        } catch (TableStoreException exception) {
            assertEquals(errorCode, exception.getErrorCode());
        }
    }

    private void describeAndAssertTableMeta(int dataTTL, int metaTTL, boolean allowUpdateAttributes) {
        DescribeTimeseriesTableRequest describeTimeseriesTableRequest = new DescribeTimeseriesTableRequest(testTable);
        DescribeTimeseriesTableResponse describeTimeseriesTableResponse = client.describeTimeseriesTable(describeTimeseriesTableRequest);
        TimeseriesTableMeta meta = describeTimeseriesTableResponse.getTimeseriesTableMeta();

        assertEquals(testTable, meta.getTimeseriesTableName());
        assertEquals("CREATED", meta.getStatus());
        assertEquals(dataTTL, meta.getTimeseriesTableOptions().getTimeToLive());
        assertEquals(metaTTL, meta.getTimeseriesMetaOptions().getMetaTimeToLive());
        assertEquals(allowUpdateAttributes, meta.getTimeseriesMetaOptions().getAllowUpdateAttributes());
    }

    private void waitCache() {
        try {
            Thread.sleep(35 * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCreateTimeseriesTableWithTTL() {
        {
            deleteTable(testTable);

            // create table with default options
            createTableWithTTL(null, null, null);
            describeAndAssertTableMeta(-1, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(-1, -1, true);
            describeAndAssertTableMeta(-1, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(-1, -1, false);
            describeAndAssertTableMeta(-1, -1, false);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, null, null);
            describeAndAssertTableMeta(10000000, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, -1, null);
            describeAndAssertTableMeta(10000000, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, null, false);
            describeAndAssertTableMeta(10000000, -1, false);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, -1, true);
            describeAndAssertTableMeta(10000000, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, 10000000, false);
            describeAndAssertTableMeta(10000000, 10000000, false);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(10000000, 20000000, false);
            describeAndAssertTableMeta(10000000, 20000000, false);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(null, 10000000, null, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(10000000, 10000000, null, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(null, 10000000, false, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(10000001, 10000000, false, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(100, null, null, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(86400, 86400, false, ErrorCode.INVALID_PARAMETER);
        }
        {
            deleteTable(testTable);
            assertCreateTableWithException(86399, 86400 * 7, false, ErrorCode.INVALID_PARAMETER);
        }
    }

    @Test
    public void testUpdateTimeseriesTableWithTTL() {
        {
            deleteTable(testTable);

            createTableWithTTL(null, null, null);
            describeAndAssertTableMeta(-1, -1, true);

            updateTableWithTTL(86400, null, null);
            describeAndAssertTableMeta(86400, -1, true);

            updateTableWithTTL(86400 * 7, null, null);
            describeAndAssertTableMeta(86400 * 7, -1, true);

            updateTableWithTTL(null, -1, null);
            describeAndAssertTableMeta(86400 * 7, -1, true);

            updateTableWithTTL(null, -1, false);
            describeAndAssertTableMeta(86400 * 7, -1, false);

            updateTableWithTTL(null, null, true);
            describeAndAssertTableMeta(86400 * 7, -1, true);

            updateTableWithTTL(null, 10000000, false);
            describeAndAssertTableMeta(86400 * 7, 10000000, false);

            updateTableWithTTL(null, -1, false);
            describeAndAssertTableMeta(86400 * 7, -1, false);

            updateTableWithTTL(null, 10000000, null);
            describeAndAssertTableMeta(86400 * 7, 10000000, false);

            updateTableWithTTL(null, -1, true);
            describeAndAssertTableMeta(86400 * 7, -1, true);
        }
        {
            deleteTable(testTable);

            createTableWithTTL(null, null, null);
            describeAndAssertTableMeta(-1, -1, true);

            assertUpdateTableWithException(86400, -1, null, ErrorCode.INVALID_PARAMETER);
            assertUpdateTableWithException(86400, null, false, ErrorCode.INVALID_PARAMETER);
            assertUpdateTableWithException(86400, -1, false, ErrorCode.INVALID_PARAMETER);

            assertUpdateTableWithException(null, 10000000, false, ErrorCode.INVALID_PARAMETER);

            updateTableWithTTL(10000000, null, null);
            describeAndAssertTableMeta(10000000, -1, true);
            updateTableWithTTL(null, 20000000, false);
            describeAndAssertTableMeta(10000000, 20000000, false);

            assertUpdateTableWithException(null, null, true, ErrorCode.INVALID_PARAMETER);
            assertUpdateTableWithException(30000000, null, null, ErrorCode.INVALID_PARAMETER);
            assertUpdateTableWithException(null, 10000000 - 1, null, ErrorCode.INVALID_PARAMETER);

            updateTableWithTTL(null, -1, false);
            describeAndAssertTableMeta(10000000, -1, false);
            updateTableWithTTL(30000000, null, null);
            describeAndAssertTableMeta(30000000, -1, false);
        }
    }

    @Test
    public void testSettingAllowUpdateAttributes() {
        deleteTable(testTable);
        createTableWithTTL(-1, -1, false);
        describeAndAssertTableMeta(-1, -1, false);
        waitCache();
        {
            Map<String, String> attrs = new HashMap<String, String>();
            attrs.put("attr1", "attr_value1");
            attrs.put("attr2", "attr_value2");
            List<TimeseriesMeta> timeseriesMetaList = new ArrayList<TimeseriesMeta>();
            for (int i = 0; i < 10; i++) {
                TimeseriesKey timeseriesKey = new TimeseriesKey("test_measure", "update_source" + i, null);
                TimeseriesMeta meta = new TimeseriesMeta(timeseriesKey);
                meta.setAttributes(attrs);
                timeseriesMetaList.add(meta);
            }
            UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest = new UpdateTimeseriesMetaRequest(testTable);
            updateTimeseriesMetaRequest.setMetas(timeseriesMetaList);
            try {
                client.updateTimeseriesMeta(updateTimeseriesMetaRequest);
                fail();
            } catch (TableStoreException ex) {
                assertEquals(ErrorCode.INVALID_PARAMETER, ex.getErrorCode());
            }
        }
        {
            List<TimeseriesMeta> timeseriesMetaList = new ArrayList<TimeseriesMeta>();
            for (int i = 0; i < 10; i++) {
                TimeseriesKey timeseriesKey = new TimeseriesKey("test_measure", "update_source" + i, null);
                TimeseriesMeta meta = new TimeseriesMeta(timeseriesKey);
                timeseriesMetaList.add(meta);
            }
            UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest = new UpdateTimeseriesMetaRequest(testTable);
            updateTimeseriesMetaRequest.setMetas(timeseriesMetaList);
            client.updateTimeseriesMeta(updateTimeseriesMetaRequest);
        }
        updateTableWithTTL(null, null, true);
        describeAndAssertTableMeta(-1, -1, true);
        waitCache();
        {
            Map<String, String> attrs = new HashMap<String, String>();
            attrs.put("attr1", "attr_value1");
            attrs.put("attr2", "attr_value2");
            List<TimeseriesMeta> timeseriesMetaList = new ArrayList<TimeseriesMeta>();
            for (int i = 0; i < 10; i++) {
                TimeseriesKey timeseriesKey = new TimeseriesKey("test_measure", "update_source" + i, null);
                TimeseriesMeta meta = new TimeseriesMeta(timeseriesKey);
                meta.setAttributes(attrs);
                timeseriesMetaList.add(meta);
            }
            UpdateTimeseriesMetaRequest updateTimeseriesMetaRequest = new UpdateTimeseriesMetaRequest(testTable);
            updateTimeseriesMetaRequest.setMetas(timeseriesMetaList);
            client.updateTimeseriesMeta(updateTimeseriesMetaRequest);
        }
    }
}
