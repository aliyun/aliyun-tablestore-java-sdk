package com.alicloud.openservices.tablestore.core.http;

import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.TestCase;
import org.junit.Assert;


public class CachedDnsResolverTest extends TestCase {
    public void testResolve() throws InterruptedException, UnknownHostException {
        CachedDnsResolver dnsResolver = new CachedDnsResolver(100, 10, 2);
        try {
            String domain = "taobao.com";
            InetAddress[] resolvedAddresses = dnsResolver.resolve(domain);
            assertEquals(1, dnsResolver.cacheSize());

            for (InetAddress address : resolvedAddresses) {
                Assert.assertNotNull(address.getHostAddress());
            }

            //Thread.sleep(13000);
            //CacheStats cacheStats = dnsResolver.cacheStat();
            //assertEquals(1, cacheStats.missCount());
            //dnsResolver.resolve(domain);
            //assertEquals(1, dnsResolver.cacheSize());
            //Thread.sleep(3000);
            //assertEquals(0, dnsResolver.cacheSize());
        } finally {
            dnsResolver.clear();
        }
    }
}