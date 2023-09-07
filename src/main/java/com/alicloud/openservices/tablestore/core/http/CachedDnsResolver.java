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
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedDnsResolver extends SystemDefaultDnsResolver {
    private final Logger logger = LoggerFactory.getLogger(CachedDnsResolver.class);
    private static final int SCHEDULED_CORE_POOL_SIZE = 2;
    private final Cache<String, InetAddress[]> dnsCache;

    public CachedDnsResolver(int maxSize, int expireAfterWriteSec, int refreshAfterWriteSec) {
        RemovalListener<String, InetAddress[]> rmListener = new RemovalListener<String, InetAddress[]>() {
            @Override
            public void onRemoval(RemovalNotification<String, InetAddress[]> notify) {
                if (logger.isDebugEnabled()) {
                    logger.debug("dns cache remove host key: {}, reason: {}", notify.getKey(), notify.getCause().name());
                }
            }
        };

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(SCHEDULED_CORE_POOL_SIZE, new ThreadFactory() {
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
                        if (logger.isDebugEnabled()) {
                            logger.debug("dns cache load, host: {}", host);
                        }
                        return CachedDnsResolver.super.resolve(host);
                    }
                }, scheduler));
    }

    public long cacheSize() {
        return dnsCache.size();
    }

    public CacheStats cacheStat() {
        return dnsCache.stats();
    }

    @Override
    public InetAddress[] resolve(String host) throws UnknownHostException {
        // 需要 getIfPresent and put. get(key, Callable) 会覆盖cache对象上的load和reload定义
        InetAddress[] cached = dnsCache.getIfPresent(host);
        if (logger.isDebugEnabled()) {
            logger.debug("dns resolve, host: {}, has cached: {}", host, cached != null);
        }
        if (cached == null) {
            InetAddress[] realtime = super.resolve(host);
            if (realtime != null) {
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
