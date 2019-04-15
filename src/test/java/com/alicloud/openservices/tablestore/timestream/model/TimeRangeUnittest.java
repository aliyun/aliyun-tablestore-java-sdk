package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimeRangeUnittest {
    @Test
    public void testRange() {
        long begin = (long)(Math.random() * 100000);
        long end = begin + 1;
        TimeRange timeRange1 = TimeRange.range(begin, end, TimeUnit.SECONDS);
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(begin), timeRange1.getBeginTime());
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(end), timeRange1.getEndTime());

        try {
            TimeRange.range(begin, begin, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
    }

    @Test
    public void testBefore() {
        long end = (long)(Math.random() * 100000);
        TimeRange timeRange1 = TimeRange.before(end, TimeUnit.SECONDS);
        Assert.assertEquals(timeRange1.getBeginTime(), 0);
        Assert.assertEquals(timeRange1.getEndTime(), TimeUnit.SECONDS.toMicros(end));

        try {
            TimeRange.before(-1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
    }

    @Test
    public void testAfter() {
        long begin = (long)(Math.random() * 100000);
        TimeRange timeRange1 = TimeRange.after(begin, TimeUnit.SECONDS);
        Assert.assertEquals(timeRange1.getEndTime(), Long.MAX_VALUE);
        Assert.assertEquals(timeRange1.getBeginTime(), TimeUnit.SECONDS.toMicros(begin));

        try {
            TimeRange.after(-1, TimeUnit.SECONDS);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
    }

    @Test
    public void testLatest() {
        long now = System.currentTimeMillis();
        long latestHour = 1;
        TimeRange timeRange1 = TimeRange.latest(latestHour, TimeUnit.HOURS);
        long newTime = System.currentTimeMillis();
        Assert.assertTrue(timeRange1.getBeginTime() >= (now * 1000 - TimeUnit.HOURS.toMicros(1)));
        Assert.assertTrue(timeRange1.getBeginTime() <= (newTime * 1000 - TimeUnit.HOURS.toMicros(1)));
        Assert.assertEquals(timeRange1.getEndTime(), Long.MAX_VALUE);
    }
}
