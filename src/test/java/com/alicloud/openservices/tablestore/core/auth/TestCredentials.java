package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.common.TestUtil;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.*;

public class TestCredentials {

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
}