package com.alicloud.openservices.tablestore.timeserieswriter.unittest;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class GroupTest {
    private String tableName = "testGroup";

    @Test
    public void testGroupOnComplete() {
        int groupSize = 200;
        String exceptionMessage = "mock exception";

        TimeseriesGroup group = new TimeseriesGroup(groupSize);
        for (int i = 0; i < groupSize; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            if (i % 2 == 0) {
                group.failedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName));
            }

        }

        try {
            TimeseriesWriterResult result = group.getFuture().get();

            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());
            Assert.assertEquals(result.getTotalCount(), groupSize);

            Assert.assertEquals(result.getSucceedRows().size(), groupSize / 2);
            for (TimeseriesWriterResult.TimeseriesRowChangeStatus status : result.getSucceedRows()) {
                Assert.assertTrue(status.isSucceed());
                Assert.assertNull(status.getException());
                Assert.assertNotNull(status.getTimeseriesTableRow());
            }

            Assert.assertEquals(result.getFailedRows().size(), groupSize / 2);
            for (TimeseriesWriterResult.TimeseriesRowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertNotNull(status.getException());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGroupOnCompleteWithDirtyRow() {
        int groupSize = 200;
        int dirtyIndex = 100;
        String exceptionMessage = "DirtyRow";

        TimeseriesGroup group = new TimeseriesGroup(groupSize);
        for (int i = 0; i < groupSize; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            if (i == dirtyIndex) {
                group.failedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName));
            }

        }

        try {
            TimeseriesWriterResult result = group.getFuture().get();

            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());
            Assert.assertEquals(result.getTotalCount(), groupSize);

            Assert.assertEquals(result.getSucceedRows().size(), groupSize - 1);
            for (TimeseriesWriterResult.TimeseriesRowChangeStatus status : result.getSucceedRows()) {
                Assert.assertTrue(status.isSucceed());
                Assert.assertNull(status.getException());
                Assert.assertNotNull(status.getTimeseriesTableRow());
            }

            Assert.assertEquals(result.getFailedRows().size(), 1);
            for (TimeseriesWriterResult.TimeseriesRowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertNotNull(status.getException());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
            }

        } catch (Exception e) {
            Assert.fail();
        }
    }


    @Test
    public void testGroupFinishMore() {
        int groupSize = 200;
        String exceptionMessage = "mock exception";

        TimeseriesGroup group = new TimeseriesGroup(groupSize);
        for (int i = 0; i < groupSize; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);
            if (i % 3 == 0) {
                group.failedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName), new Exception(exceptionMessage));
            } else {
                group.succeedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName));
            }

        }

        try {
            group.succeedOneRow(null);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage().split("] ")[1], "WriterResult shouldn't finish more rows than total count");
            Assert.assertEquals(e.getMessage().split("] ")[0], "[" + group.getGroupId());
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testGroupFinishLess() {
        int groupSize = 200;
        String exceptionMessage = "Mock Timeout";

        TimeseriesGroup group = new TimeseriesGroup(groupSize);
        for (int i = 0; i < groupSize - 1; i++) {
            Map<String, String> tags = new HashMap<String, String>();
            tags.put("region", "hangzhou");
            tags.put("os", "Ubuntu16.04");
            TimeseriesKey timeseriesKey = new TimeseriesKey("cpu" + i, "host_" + i, tags);

            group.succeedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName));
        }

        try {
            group.getFuture().get(1000, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException e) {

        } catch (Exception e) {
            Assert.fail();
        }

        Map<String, String> tags = new HashMap<String, String>();
        tags.put("region", "hangzhou");
        tags.put("os", "Ubuntu16.04");
        TimeseriesKey timeseriesKey = new TimeseriesKey("timeoutValue", "timeoutData", tags);

        group.failedOneRow(new TimeseriesTableRow(new TimeseriesRow(timeseriesKey), tableName), new Exception(exceptionMessage));

        try {
            TimeseriesWriterResult result = group.getFuture().get();
            Assert.assertTrue(result.isAllFinished());
            Assert.assertFalse(result.isAllSucceed());

            Assert.assertEquals(result.getFailedRows().size(), 1);
            for (TimeseriesWriterResult.TimeseriesRowChangeStatus status : result.getFailedRows()) {
                Assert.assertFalse(status.isSucceed());
                Assert.assertEquals(status.getException().getMessage(), exceptionMessage);
                Assert.assertEquals(status.getTimeseriesTableRow().getTimeseriesRow().getTimeseriesKey().getMeasurementName(), "timeoutValue");
            }
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
