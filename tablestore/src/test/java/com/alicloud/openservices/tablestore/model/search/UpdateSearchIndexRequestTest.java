package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.model.StorageClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class UpdateSearchIndexRequestTest extends BaseSearchTest {

    @Test
    public void testSetStorageClass() {
        UpdateSearchIndexRequest request = new UpdateSearchIndexRequest("table", "index");
        Assert.assertNull(request.getStorageClass());

        Assert.assertSame(request, request.setStorageClass(StorageClass.SC_IA));
        Assert.assertEquals(StorageClass.SC_IA, request.getStorageClass());

        request.setStorageClass(StorageClass.SC_STANDARD);
        Assert.assertEquals(StorageClass.SC_STANDARD, request.getStorageClass());
    }

    @Test
    public void testSetTTL() {
        // normal
        {
            String table = randomString(10);
            String index = randomString(10);
            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest(table, index);
            int ttl = random().nextInt(50) + 1;
            TimeUnit timeUnit = randomFrom(TimeUnit.values());
            request.setTimeToLive(ttl, timeUnit);
            Assert.assertEquals(request.getTimeToLive().intValue(), (int) timeUnit.toSeconds(ttl));
        }
        // -1
        {
            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest("table", "index");
            request.setTimeToLiveInDays(-1);
            Assert.assertEquals(-1, request.getTimeToLive().intValue());
        }
        // not set
        {
            String table = randomString(10);
            String index = randomString(10);
            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest(table, index);
            Assert.assertNull(request.getTimeToLive());
        }
        // illegal ttl
        {
            String table = randomString(10);
            String index = randomString(10);
            UpdateSearchIndexRequest request = new UpdateSearchIndexRequest(table, index);
            try {
                request.setTimeToLive(Integer.MAX_VALUE, TimeUnit.MINUTES);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), ArithmeticException.class);
            }
            try {
                request.setTimeToLive(0, TimeUnit.DAYS);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
                Assert.assertEquals("The value of timeToLive can be -1 or any positive value.", e.getMessage());
            }
            try {
                request.setTimeToLive(-2, TimeUnit.DAYS);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals("The value of timeToLive can be -1 or any positive value.", e.getMessage());
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
            }
        }
    }
}
