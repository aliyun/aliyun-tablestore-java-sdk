package com.alicloud.openservices.tablestore.tunnel.unittest;

import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import junit.framework.TestCase;


public class TunnelClientCreateTest extends TestCase {
    private static final String Endpoint = "https://test.cn-hangzhou.ots.aliyuncs.com";
    private static final String AccessId = "test";
    private static final String AccessKey = "test";
    private static final String InstanceName = "test";

    @Override
    protected void setUp() throws Exception {
        System.out.println("begin setup");
    }

    @Override
    protected void tearDown() throws Exception {
        System.out.println("begin teardown");
    }

    public void testCreateTunnelClient() {
        TunnelClient tunnelClient = new TunnelClient(Endpoint, AccessId, AccessKey, InstanceName, null, null, null);

        assertEquals(Endpoint, tunnelClient.getEndpoint());
        assertEquals(InstanceName, tunnelClient.getInstanceName());
        assertFalse(tunnelClient.getClientConfig().isEnableResponseValidation());

        tunnelClient.shutdown();
    }

    public void testCreateTunnelClientByCredentialsProvider() {
        CredentialsProvider credsProvider = new DefaultCredentialProvider(AccessId, AccessKey, null);
        ResourceManager resourceManager = new ResourceManager(null, null);
        TunnelClient tunnelClient = new TunnelClient(Endpoint, credsProvider, InstanceName, null, resourceManager);

        assertEquals(Endpoint, tunnelClient.getEndpoint());
        assertEquals(InstanceName, tunnelClient.getInstanceName());
        assertFalse(tunnelClient.getClientConfig().isEnableResponseValidation());

        tunnelClient.shutdown();
    }
}