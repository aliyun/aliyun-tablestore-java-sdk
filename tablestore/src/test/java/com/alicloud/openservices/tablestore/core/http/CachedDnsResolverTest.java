package com.alicloud.openservices.tablestore.core.http;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import junit.framework.TestCase;
import com.aliyun.ots.thirdparty.org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

    public void testAsyncResolve() throws UnknownHostException, InterruptedException {
        TestCachedDnsResolver resolver = new TestCachedDnsResolver(
                100, 20, 3);

        for (int i = 0; i < 10; ++i) {
            long start = System.currentTimeMillis();
            InetAddress[] resolve = resolver.resolve("taobao.com");
            long end = System.currentTimeMillis();
            System.out.println(end - start);

            Thread.sleep(2000);
        }
    }
}

class BaseResolver extends SystemDefaultDnsResolver {

    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return super.resolve(host);
    }
}

class TestCachedDnsResolver extends BaseResolver {
    private final Logger logger = LoggerFactory.getLogger(CachedDnsResolverTest.class);
    private final Cache<String, InetAddress[]> dnsCache;

    public TestCachedDnsResolver(int maxSize, int expireAfterWriteSec, int refreshAfterWriteSec) {
        RemovalListener<String, InetAddress[]> rmListener = new RemovalListener<String, InetAddress[]>() {
            @Override
            public void onRemoval(RemovalNotification<String, InetAddress[]> notify) {
                logger.info("dns cache remove host key: {}, reason: {}", notify.getKey(), notify.getCause().name());
            }
        };

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("refresh-dns-cache-" + thread.getId());
                return thread;
            }
        });
        dnsCache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .weakKeys()
                .expireAfterWrite(expireAfterWriteSec, TimeUnit.SECONDS)
                .refreshAfterWrite(refreshAfterWriteSec, TimeUnit.SECONDS)
                .removalListener(rmListener)
                .build(CacheLoader.asyncReloading(new CacheLoader<String, InetAddress[]>() {
                    @Override
                    public InetAddress[] load(String host) throws Exception {
                        logger.info("dns cache load, host: {}", host);
                        return TestCachedDnsResolver.super.resolve(host);
                    }
                }, scheduler));
    }


    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        InetAddress[] cached = dnsCache.getIfPresent(host);
        logger.info("dns resolve, host: {}, has cached: {}", host, cached!=null);
        if (cached==null) {
            InetAddress[] realtime = super.resolve(host);
            if (realtime!=null) {
                dnsCache.put(host, realtime);
            }
            return realtime;
        }
        return cached;
    }

    public void clear() {
        dnsCache.cleanUp();
    }
}
