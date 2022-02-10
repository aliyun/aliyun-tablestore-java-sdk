package com.alicloud.openservices.tablestore.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TableOptionsTest {

    @Test
    public void setTimeToLive() {
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
}