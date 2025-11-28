package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.common.TestUtil;
import org.junit.Test;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class CredentialsTest {
    private Random random = new Random();

    @Test
    public void testBasicCredentials() throws Exception {
        BasicCredentials bc = new BasicCredentials("accessid", "accesskey", "token", 0);
        assertEquals(bc.getAccessKeyId(), "accessid");
        assertEquals(bc.getAccessKeySecret(), "accesskey");
        assertEquals(bc.getSecurityToken(), "token");

        assertFalse(bc.willSoonExpire());

        bc = new BasicCredentials("accessid", "accesskey", "token").withExpiredDuration(10).withExpiredFactor(0.8);
        assertFalse(bc.willSoonExpire());

        Thread.sleep(2000);
        assertFalse(bc.willSoonExpire());

        Thread.sleep(6100);
        assertTrue(bc.willSoonExpire());

        Thread.sleep(3000);
        assertTrue(bc.willSoonExpire());
    }

    @Test
    public void testBasicCredentialsDefaultValue() throws Exception {
        BasicCredentials bc = new BasicCredentials("accessid", "accesskey", "token");
        assertEquals(bc.getAccessKeyId(), "accessid");
        assertEquals(bc.getAccessKeySecret(), "accesskey");
        assertEquals(bc.getSecurityToken(), "token");
        assertEquals(bc.expiredDurationSeconds, 0);
        assertFalse(bc.willSoonExpire());

        Thread.sleep(10000);
        assertFalse(bc.willSoonExpire());

        bc.withExpiredDuration(10);
        assertTrue(bc.willSoonExpire());
    }

    @Test
    public void testDefaultCredentialProvider() {
        try {
            CredentialsProviderFactory.newDefaultCredentialProvider("", "a");
            fail("expect failure");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key id should not be null or empty.");
        }
        try {
            CredentialsProviderFactory.newDefaultCredentialProvider(null, "a");
            fail("expect failure");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key id should not be null or empty.");
        }
        try {
            CredentialsProviderFactory.newDefaultCredentialProvider("a", "");
            fail("expect failure");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key secret should not be null or empty.");
        }
        try {
            CredentialsProviderFactory.newDefaultCredentialProvider("a", null);
            fail("expect failure");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key secret should not be null or empty.");
        }

        try {
            CredentialsProviderFactory.newDefaultCredentialProvider("%", "ad9j23JDS");
            fail("expect failure");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "The access key id is invalid: %");
        }

        CredentialsProvider cp = CredentialsProviderFactory.newDefaultCredentialProvider("accessid", "accesskey", "token");
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");
        assertEquals(cp.getCredentials().getSecurityToken(), "token");

        cp = CredentialsProviderFactory.newDefaultCredentialProvider("accessid", "accesskey");
        assertEquals(cp.getCredentials().getSecurityToken(), null);
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");

        cp = CredentialsProviderFactory.newDefaultCredentialProvider("STS.adjxau239x", "ad9j23JDS");
        assertEquals(cp.getCredentials().getSecurityToken(), null);
        assertEquals(cp.getCredentials().getAccessKeyId(), "STS.adjxau239x");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "ad9j23JDS");
    }

    @Test
    public void testEnvCredentialsProvider() throws Exception {
        TestUtil.injectEnvironmentVariable(AuthUtils.ACCESS_KEY_ENV_VAR, "accessid");
        TestUtil.injectEnvironmentVariable(AuthUtils.SECRET_KEY_ENV_VAR, "accesskey");

        CredentialsProvider cp = CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");

        TestUtil.injectEnvironmentVariable(AuthUtils.SESSION_TOKEN_ENV_VAR, "token");
        cp = CredentialsProviderFactory.newEnvironmentVariableCredentialsProvider();
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");
        assertEquals(cp.getCredentials().getSecurityToken(), "token");
    }

    @Test
    public void testSystemPropertiesCredentialsProvider() throws Exception {
        try {
            CredentialsProvider cp = CredentialsProviderFactory.newSystemPropertiesCredentialsProvider();
            cp.getCredentials();
            fail("expect failure.");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key id should not be null or empty.");
        }

        try {
            System.setProperty(AuthUtils.ACCESS_KEY_SYSTEM_PROPERTY, "accessid");
            CredentialsProvider cp = CredentialsProviderFactory.newSystemPropertiesCredentialsProvider();
            cp.getCredentials();
            fail("expect failure.");
        } catch (InvalidCredentialsException e) {
            assertEquals(e.getMessage(), "Access key secret should not be null or empty.");
        }

        System.setProperty(AuthUtils.ACCESS_KEY_SYSTEM_PROPERTY, "accessid");
        System.setProperty(AuthUtils.SECRET_KEY_SYSTEM_PROPERTY, "accesskey");
        CredentialsProvider cp = CredentialsProviderFactory.newSystemPropertiesCredentialsProvider();
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");
        assertEquals(cp.getCredentials().getSecurityToken(), null);

        System.setProperty(AuthUtils.SESSION_TOKEN_SYSTEM_PROPERTY, "token");
        cp = CredentialsProviderFactory.newSystemPropertiesCredentialsProvider();
        assertEquals(cp.getCredentials().getAccessKeyId(), "accessid");
        assertEquals(cp.getCredentials().getAccessKeySecret(), "accesskey");
        assertEquals(cp.getCredentials().getSecurityToken(), "token");
    }

    @Test
    public void testInstanceProfileCredentialsBasic() {
        InstanceProfileCredentials c = new InstanceProfileCredentials("accessid", "accesskey", "token", "2018-12-25T07:06:37Z");
        assertEquals(c.expiredDurationSeconds, AuthUtils.DEFAULT_ECS_SESSION_TOKEN_DURATION_SECONDS);
        assertEquals(c.getAccessKeyId(), "accessid");
        assertEquals(c.getAccessKeySecret(), "accesskey");
        assertEquals(c.getSecurityToken(), "token");
    }

    @Test
    public void testInstanceProfileCredentials() throws Exception {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        parser.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date expireTime = new Date(System.currentTimeMillis() + 10000); // 10s later
        String expireTimeStr = parser.format(expireTime);

        InstanceProfileCredentials c = new InstanceProfileCredentials("accessid", "accesskey", "token", expireTimeStr);
        c.withExpiredDuration(10).withExpiredFactor(0.6).withRefreshIntervalInMilliseconds(2000);

        Thread.sleep(1000);
        assertFalse(c.isExpired());
        assertFalse(c.willSoonExpire());
        assertTrue(c.shouldRefresh());

        Thread.sleep(5100);
        assertFalse(c.isExpired());
        assertTrue(c.willSoonExpire());
        assertTrue(c.shouldRefresh());

        Thread.sleep(2000);
        assertTrue(c.isExpired());
        assertTrue(c.willSoonExpire());
        assertTrue(c.shouldRefresh());

        Thread.sleep(5000);
        assertTrue(c.isExpired());
        assertTrue(c.willSoonExpire());
        assertTrue(c.shouldRefresh());
    }

    /**
     * Test concurrent update scenario with date change simulation
     * This test simulates the real bug scenario by:
     * 1. Using reflection to set signingDate to yesterday
     * 2. This causes dataNow != signingDate, triggering updateV4Signature()
     * 3. Multiple threads concurrently access credentials during update
     * 4. Verify getKeyDatePair() always returns consistent pairs (the fix)
     * 5. Verify separate get calls could have issues before fix (but now fixed)
     */
    @Test
    public void testConcurrentUpdateWithDateChange() throws Exception {
        final ServiceCredentials serviceCredentials = new DefaultCredentials(
                getRandomString(24),
                getRandomString(30)
        );
        final V4Credentials v4Credentials = V4Credentials.createByServiceCredentials(
                serviceCredentials,
                "cn-test"
        );
        // CRITICAL: Use reflection to set signingDate to yesterday
        // This simulates the scenario where date has changed
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.DAY_OF_MONTH, -1);
        String yesterday = dateFormat.format(cal.getTime());

        Field signingDateField = V4Credentials.class.getDeclaredField("signingDate");
        signingDateField.setAccessible(true);
        signingDateField.set(v4Credentials, yesterday);
        System.out.println("Set signingDate to yesterday: " + yesterday);
        System.out.println("Current date would be: " + dateFormat.format(cal.getTime()));
        final int threadCount = 20;
        final int iterationsPerThread = 500;
        final java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch endLatch = new java.util.concurrent.CountDownLatch(threadCount);
        final java.util.concurrent.ConcurrentHashMap<String, String> pairKeyToDate =
                new java.util.concurrent.ConcurrentHashMap<String, String>();
        final java.util.concurrent.atomic.AtomicInteger pairMismatchCount =
                new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger separateGetInconsistency =
                new java.util.concurrent.atomic.AtomicInteger(0);
        for (int t = 0; t < threadCount; t++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();

                        for (int i = 0; i < iterationsPerThread; i++) {
                            // Method 1: Use getKeyDatePair() - should ALWAYS be consistent
                            com.alicloud.openservices.tablestore.core.utils.Pair<String, String> pair =
                                    v4Credentials.getKeyDatePair();
                            String pairKey = pair.getFirst();
                            String pairDate = pair.getSecond();

                            // Track all key-date mappings
                            String existingDate = pairKeyToDate.putIfAbsent(pairKey, pairDate);
                            if (existingDate != null && !existingDate.equals(pairDate)) {
                                pairMismatchCount.incrementAndGet();
                                System.err.println("CRITICAL BUG: Same key with different dates!");
                                System.err.println("  Key: " + pairKey);
                                System.err.println("  Existing date: " + existingDate);
                                System.err.println("  New date: " + pairDate);
                            }

                            // Method 2: Separate get calls - simulate the problematic pattern
                            String separateKey = v4Credentials.getAccessKeySecret();
                            // Tiny delay to increase chance of interleaving
                            if (i % 10 == 0) {
                                Thread.yield();
                            }
                            String separateDate = v4Credentials.getSigningDate();

                            // Check if separate calls are consistent with the pair
                            // With the fix, both methods should give consistent results
                            if (!pairKeyToDate.containsKey(separateKey)) {
                                pairKeyToDate.put(separateKey, separateDate);
                            } else {
                                String mappedDate = pairKeyToDate.get(separateKey);
                                if (!mappedDate.equals(separateDate)) {
                                    separateGetInconsistency.incrementAndGet();
                                    System.err.println("Separate get inconsistency detected!");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        pairMismatchCount.incrementAndGet();
                    } finally {
                        endLatch.countDown();
                    }
                }
            }).start();
        }
        startLatch.countDown();
        endLatch.await(60, java.util.concurrent.TimeUnit.SECONDS);
        // Verify results
        if (pairMismatchCount.get() > 0) {
            throw new AssertionError("getKeyDatePair() found " + pairMismatchCount.get() +
                    " inconsistencies - THE FIX FAILED!");
        }

        if (separateGetInconsistency.get() > 0) {
            throw new AssertionError("Separate get calls found " + separateGetInconsistency.get() +
                    " inconsistencies - THE FIX FAILED!");
        }

        System.out.println("\n✅ Concurrent update test PASSED with date change simulation:");
        System.out.println("   - Threads: " + threadCount);
        System.out.println("   - Iterations per thread: " + iterationsPerThread);
        System.out.println("   - Total operations: " + (threadCount * iterationsPerThread));
        System.out.println("   - Unique key-date pairs observed: " + pairKeyToDate.size());
        System.out.println("   - getKeyDatePair() mismatches: " + pairMismatchCount.get());
        System.out.println("   - Separate get inconsistencies: " + separateGetInconsistency.get());
        System.out.println("   ✅ Fix successfully prevents race conditions!");
    }
    /**
     * Test the exact race condition scenario described in the bug report:
     * - Thread A calls getAccessKeySecret() and gets v4SigningAccessKey
     * - Thread B calls getAccessKeySecret() and updates both signingDate and v4SigningAccessKey
     * - Thread A then calls getSigningDate() and gets the NEW signingDate
     * - Result: Thread A has mismatched key (old) and date (new)
     *
     * This test verifies that the fix (using Pair + synchronized block) prevents this issue.
     */
    @Test
    public void testRaceConditionPreventedByFix() throws Exception {
        final ServiceCredentials serviceCredentials = new DefaultCredentials(
                getRandomString(24),
                getRandomString(30)
        );
        final V4Credentials v4Credentials = V4Credentials.createByServiceCredentials(
                serviceCredentials,
                "cn-test"
        );
        // Set signingDate to yesterday to trigger update
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.DAY_OF_MONTH, -1);
        String yesterday = dateFormat.format(cal.getTime());

        Field signingDateField = V4Credentials.class.getDeclaredField("signingDate");
        signingDateField.setAccessible(true);
        signingDateField.set(v4Credentials, yesterday);
        final java.util.concurrent.atomic.AtomicBoolean raceConditionDetected =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        final java.util.concurrent.ConcurrentHashMap<String, String> keyDateMapping =
                new java.util.concurrent.ConcurrentHashMap<String, String>();
        Thread[] threads = new Thread[50];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int j = 0; j < 100; j++) {
                            // Get key and date using the atomic pair method
                            com.alicloud.openservices.tablestore.core.utils.Pair<String, String> pair =
                                    v4Credentials.getKeyDatePair();
                            String key = pair.getFirst();
                            String date = pair.getSecond();

                            // Verify this key always maps to the same date
                            String previousDate = keyDateMapping.putIfAbsent(key, date);
                            if (previousDate != null && !previousDate.equals(date)) {
                                raceConditionDetected.set(true);
                                System.err.println("RACE CONDITION: key " + key.substring(0, 10) +
                                        "... mapped to multiple dates!");
                            }

                            // Also verify separate calls are consistent
                            String separateKey = v4Credentials.getAccessKeySecret();
                            String separateDate = v4Credentials.getSigningDate();

                            if (keyDateMapping.containsKey(separateKey)) {
                                String expectedDate = keyDateMapping.get(separateKey);
                                if (!expectedDate.equals(separateDate)) {
                                    raceConditionDetected.set(true);
                                    System.err.println("RACE CONDITION: separate get returned inconsistent data!");
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        raceConditionDetected.set(true);
                    }
                }
            });
            threads[i].start();
        }
        // Wait for all threads
        for (Thread thread : threads) {
            thread.join(2000);
        }
        if (raceConditionDetected.get()) {
            throw new AssertionError("Race condition detected! Same key was associated with different dates!");
        }

        System.out.println("\n✅ Race condition prevention test PASSED:");
        System.out.println("   - " + threads.length + " concurrent threads");
        System.out.println("   - " + keyDateMapping.size() + " unique key(s) observed");
        System.out.println("   - No race conditions detected");
        System.out.println("   ✅ The fix successfully prevents the bug!");
    }

    public String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}