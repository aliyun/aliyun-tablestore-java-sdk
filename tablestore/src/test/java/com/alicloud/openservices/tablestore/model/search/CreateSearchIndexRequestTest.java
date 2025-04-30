package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class CreateSearchIndexRequestTest extends BaseSearchTest {

    @Test
    public void testSetTTL() {
        // normal
        {
            String table = randomString(10);
            String index = randomString(10);
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(table, index);
            int ttl = random().nextInt(50) + 1;
            request.setTimeToLiveInDays(ttl);
            Assert.assertEquals(request.getTimeToLive().intValue(), (int) TimeUnit.DAYS.toSeconds(ttl));
        }
        // -1
        {
            {
                CreateSearchIndexRequest request = new CreateSearchIndexRequest("table", "index");
                request.setTimeToLiveInDays(-1);
                Assert.assertEquals(-1, request.getTimeToLive().intValue());
            }
            {
                CreateSearchIndexRequest request = new CreateSearchIndexRequest("table", "index");
                request.setTimeToLive(-1, TimeUnit.SECONDS);
                Assert.assertEquals(-1, request.getTimeToLive().intValue());
            }
        }
        // not set
        {
            String table = randomString(10);
            String index = randomString(10);
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(table, index);
            Assert.assertNull(request.getTimeToLive());
        }
        // illegal ttl
        {
            String table = randomString(10);
            String index = randomString(10);
            CreateSearchIndexRequest request = new CreateSearchIndexRequest(table, index);
            try {
                request.setTimeToLiveInDays(Integer.MAX_VALUE);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), ArithmeticException.class);
            }
            try {
                request.setTimeToLiveInDays(0);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
                Assert.assertEquals("The value of timeToLive can be -1 or any positive value.", e.getMessage());
            }
            try {
                request.setTimeToLiveInDays(-2);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals("The value of timeToLive can be -1 or any positive value.", e.getMessage());
                Assert.assertEquals(e.getClass(), IllegalArgumentException.class);
            }
        }
    }
}