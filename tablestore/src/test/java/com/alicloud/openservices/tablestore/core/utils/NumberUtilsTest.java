package com.alicloud.openservices.tablestore.core.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class NumberUtilsTest {

    @Test
    public void longToInt() {
        // normal
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            int nextInt = random.nextInt(Integer.MAX_VALUE);
            Assert.assertEquals(nextInt, NumberUtils.longToInt(nextInt));
        }

        // too large
        for (int i = 0; i < 1000; i++) {
            int nextInt = random.nextInt(Integer.MAX_VALUE);
            try {
                int longToInt = NumberUtils.longToInt((long) nextInt + Integer.MAX_VALUE);
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals(e.getClass(), ArithmeticException.class);
                Assert.assertEquals("integer overflow", e.getMessage());
            }
        }


    }
}