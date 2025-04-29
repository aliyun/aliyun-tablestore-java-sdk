package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.common.TestUtil;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TableOptionsTest {
    // test different constructor
    @Test
    public void testConstructor() {
        for (int i = 0; i < 100; i++) {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            int maxVersions = Math.max(1, TestUtil.randomLength());
            long maxTimeDeviation = Math.abs(TestUtil.randomLong());
            boolean allowUpdate = TestUtil.randomBoolean();
            boolean updateFullRow = TestUtil.randomBoolean();

            {
                TableOptions tableOptions = new TableOptions((int)ttlSeconds);
                Assert.assertEquals(ttlSeconds, tableOptions.getTimeToLive());
            }

            {
                TableOptions tableOptions = new TableOptions((int)ttlSeconds, maxVersions);
                Assert.assertEquals(ttlSeconds, tableOptions.getTimeToLive());
                Assert.assertEquals(maxVersions, tableOptions.getMaxVersions());
            }

            {
                TableOptions tableOptions = new TableOptions((int)ttlSeconds, maxVersions, maxTimeDeviation);
                Assert.assertEquals(ttlSeconds, tableOptions.getTimeToLive());
                Assert.assertEquals(maxVersions, tableOptions.getMaxVersions());
                Assert.assertEquals(maxTimeDeviation, tableOptions.getMaxTimeDeviation());
            }

            {
                TableOptions tableOptions = new TableOptions(allowUpdate);
                Assert.assertEquals(allowUpdate, tableOptions.getAllowUpdate());
            }

            {
                TableOptions tableOptions = new TableOptions(allowUpdate, updateFullRow);
                Assert.assertEquals(allowUpdate, tableOptions.getAllowUpdate());
                Assert.assertEquals(updateFullRow, tableOptions.getUpdateFullRow());
            }
        }
    }

    @Test
    public void testGetValueException() {
        TableOptions tableOptions = new TableOptions();
        {
            Assert.assertEquals(tableOptions.hasSetTimeToLive(), false);
            TestUtil.expectThrowsAndMessages(
                    IllegalStateException.class,
                    () -> tableOptions.getTimeToLive(),
                    "The value of TimeToLive is not set."
            );
        }

        {
            Assert.assertEquals(tableOptions.hasSetMaxVersions(), false);
            TestUtil.expectThrowsAndMessages(
                    IllegalStateException.class,
                    () -> tableOptions.getMaxVersions(),
                    "The value of MaxVersions is not set."
            );
        }

        {
            Assert.assertEquals(tableOptions.hasSetMaxTimeDeviation(), false);
            TestUtil.expectThrowsAndMessages(
                    IllegalStateException.class,
                    () -> tableOptions.getMaxTimeDeviation(),
                    "The value of MaxTimeDeviation is not set."
            );
        }

        {
            Assert.assertEquals(tableOptions.hasSetAllowUpdate(), false);
            TestUtil.expectThrowsAndMessages(
                    IllegalStateException.class,
                    () -> tableOptions.getAllowUpdate(),
                    "The value of AllowUpdate is not set."
            );
        }

        {
            Assert.assertEquals(tableOptions.hasSetUpdateFullRow(), false);
            TestUtil.expectThrowsAndMessages(
                    IllegalStateException.class,
                    () -> tableOptions.getUpdateFullRow(),
                    "The value of UpdateFullRow is not set."
            );
        }
    }

    @Test
    public void testTimeToLive() {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int ttlDays = random.nextInt(10000) + 1;
            long ttlSeconds = TimeUnit.DAYS.toSeconds(ttlDays);
            TableOptions tableOptions1 = new TableOptions();
            tableOptions1.setTimeToLive((int) ttlSeconds);
            TableOptions tableOptions2 = new TableOptions();
            tableOptions2.setTimeToLiveInDays(ttlDays);
            Assert.assertEquals(tableOptions1.getTimeToLive(), tableOptions2.getTimeToLive());
        }

        // -1
        {
            TableOptions tableOptions1 = new TableOptions();
            tableOptions1.setTimeToLive(-1);
            TableOptions tableOptions2 = new TableOptions();
            tableOptions2.setTimeToLiveInDays(-1);
            Assert.assertEquals(tableOptions1.getTimeToLive(), tableOptions2.getTimeToLive());
        }
    }

    @Test
    public void testUpdateFullRow() {
        {
            for (int i = 0; i < 4; i++) {
                boolean allowUpdate = ((i >> 0) & 1) == 1;
                boolean updateFullRow = ((i >> 1) & 1) == 1;

                TableOptions tableOptions1 = new TableOptions(allowUpdate, updateFullRow);
                Assert.assertTrue(tableOptions1.hasSetUpdateFullRow());
                Assert.assertEquals(updateFullRow, tableOptions1.getUpdateFullRow());

                TableOptions tableOptions2 = new TableOptions();
                Assert.assertFalse(tableOptions2.hasSetUpdateFullRow());
                tableOptions2.setUpdateFullRow(updateFullRow);
                Assert.assertTrue(tableOptions2.hasSetUpdateFullRow());
                Assert.assertEquals(updateFullRow, tableOptions2.getUpdateFullRow());
            }
        }
    }

    @Test
    public void testToString() {
        for (int i = 0; i < 100; i++) {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            int maxVersions = Math.max(1, TestUtil.randomLength());
            long maxTimeDeviation = Math.abs(TestUtil.randomLong());
            boolean allowUpdate = TestUtil.randomBoolean();
            boolean updateFullRow = TestUtil.randomBoolean();

            String testString = "";
            TableOptions tableOptions = new TableOptions();
            testString += "TimeToLive:" + ttlSeconds;
            tableOptions.setTimeToLive((int) ttlSeconds);
            testString += ", MaxVersions:" + maxVersions;
            tableOptions.setMaxVersions(maxVersions);
            testString += ", MaxTimeDeviation:" + maxTimeDeviation;
            tableOptions.setMaxTimeDeviation(maxTimeDeviation);
            testString += ", AllowUpdate:" + allowUpdate;
            tableOptions.setAllowUpdate(allowUpdate);
            testString += ", UpdateFullRow:" + updateFullRow;
            tableOptions.setUpdateFullRow(updateFullRow);

            Assert.assertTrue(tableOptions.hasSetTimeToLive());
            Assert.assertTrue(tableOptions.hasSetMaxVersions());
            Assert.assertTrue(tableOptions.hasSetMaxTimeDeviation());
            Assert.assertTrue(tableOptions.hasSetAllowUpdate());
            Assert.assertTrue(tableOptions.hasSetUpdateFullRow());
            Assert.assertEquals(testString, tableOptions.toString());
        }
    }

    @Test
    public void testJsonize() {
        for (int i=0; i<100; i++) {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            int maxVersions = Math.max(1, TestUtil.randomLength());
            long maxTimeDeviation = Math.abs(TestUtil.randomLong());
            boolean allowUpdate = TestUtil.randomBoolean();
            boolean updateFullRow = TestUtil.randomBoolean();

            String jsonString = "";
            jsonString += "{";
            TableOptions tableOptions = new TableOptions();
            jsonString += "\"TimeToLive\": " + ttlSeconds;
            tableOptions.setTimeToLive((int) ttlSeconds);
            jsonString += ", \"MaxVersions\": " + maxVersions;
            tableOptions.setMaxVersions(maxVersions);
            jsonString += ", \"MaxTimeDeviation\": " + maxTimeDeviation;
            tableOptions.setMaxTimeDeviation(maxTimeDeviation);
            jsonString += ", \"AllowUpdate\": " + allowUpdate;
            tableOptions.setAllowUpdate(allowUpdate);
            jsonString += ", \"UpdateFullRow\": " + updateFullRow;
            tableOptions.setUpdateFullRow(updateFullRow);
            jsonString += "}";

            Assert.assertEquals(jsonString, tableOptions.jsonize());
        }

        // test set one element only
        for (int i=0; i<100; i++) {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            int maxVersions = Math.max(1, TestUtil.randomLength());
            long maxTimeDeviation = Math.abs(TestUtil.randomLong());
            boolean allowUpdate = TestUtil.randomBoolean();
            boolean updateFullRow = TestUtil.randomBoolean();

            String jsonString = "";
            jsonString += "{";
            TableOptions tableOptions = new TableOptions();

            switch (i % 5) {
                case 0:
                    jsonString += "\"TimeToLive\": " + ttlSeconds;
                    tableOptions.setTimeToLive((int) ttlSeconds);
                    break;
                case 1:
                    jsonString += "\"MaxVersions\": " + maxVersions;
                    tableOptions.setMaxVersions(maxVersions);
                    break;
                case 2:
                    jsonString += "\"MaxTimeDeviation\": " + maxTimeDeviation;
                    tableOptions.setMaxTimeDeviation(maxTimeDeviation);
                    break;
                case 3:
                    jsonString += "\"AllowUpdate\": " + allowUpdate;
                    tableOptions.setAllowUpdate(allowUpdate);
                    break;
                case 4:
                    jsonString += "\"UpdateFullRow\": " + updateFullRow;
                    tableOptions.setUpdateFullRow(updateFullRow);
                    break;
                default:
                    Assert.fail();
            }
            jsonString += "}";

            Assert.assertEquals(jsonString, tableOptions.jsonize());
        }

        // test ttl not the first element
        {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            TableOptions tableOptions = new TableOptions((int)ttlSeconds);
            StringBuilder sb = new StringBuilder();
            tableOptions.jsonizeFields(sb, false);
            String jsonString = ", \"TimeToLive\": " + (int)ttlSeconds;
            Assert.assertEquals(jsonString, sb.toString());
        }
    }

    @Test
    public void testOTSProtocolBuildAndParseTableOptions() {
        for (int i = 0; i < 100; i++) {
            long ttlSeconds = Math.abs(TestUtil.randomLong());
            int maxVersions = Math.max(1, TestUtil.randomLength());
            long maxTimeDeviation = Math.abs(TestUtil.randomLong());
            boolean allowUpdate = TestUtil.randomBoolean();
            boolean updateFullRow = TestUtil.randomBoolean();

            TableOptions tableOptions = new TableOptions();
            tableOptions.setTimeToLive((int) ttlSeconds);
            tableOptions.setMaxVersions(maxVersions);
            tableOptions.setMaxTimeDeviation(maxTimeDeviation);
            tableOptions.setAllowUpdate(allowUpdate);
            tableOptions.setUpdateFullRow(updateFullRow);

            OtsInternalApi.TableOptions tableOptionsPb = OTSProtocolBuilder.buildTableOptions(tableOptions);
            Assert.assertEquals(tableOptions.getTimeToLive(), tableOptionsPb.getTimeToLive());
            Assert.assertEquals(tableOptions.getMaxVersions(), tableOptionsPb.getMaxVersions());
            Assert.assertEquals(tableOptions.getMaxTimeDeviation(), tableOptionsPb.getDeviationCellVersionInSec());
            Assert.assertEquals(tableOptions.getAllowUpdate(), tableOptionsPb.getAllowUpdate());
            Assert.assertEquals(tableOptions.getUpdateFullRow(), tableOptionsPb.getUpdateFullRow());

            TableOptions tableOptionsParse = OTSProtocolParser.parseTableOptions(tableOptionsPb);
            Assert.assertEquals(tableOptions.jsonize(), tableOptionsParse.jsonize());
        }
    }
}