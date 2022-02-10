package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientResources {
    private static int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private AsyncServiceClient httpClient;
    private ScheduledExecutorService retryExecutor;
    private ExecutorService callbackExecutor;

    public ClientResources(ClientConfiguration config, ExecutorService callbackExecutor) {
        this.httpClient = new AsyncServiceClient(config);

        this.retryExecutor = Executors.newScheduledThreadPool(config.getRetryThreadCount(),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "tablestore-retry-scheduled-" + counter.getAndIncrement());
                    }
                });

        if (callbackExecutor != null) {
            this.callbackExecutor = callbackExecutor;
        } else {
            this.callbackExecutor = Executors.newFixedThreadPool(AVAILABLE_PROCESSORS,
                    new ThreadFactory() {
                        private final AtomicInteger counter = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "tablestore-callback-" + counter.getAndIncrement());
                        }
                    });
        }
    }

    public AsyncServiceClient getHttpClient() {
        return this.httpClient;
    }

    public ScheduledExecutorService getRetryExecutor() {
        return this.retryExecutor;
    }

    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor;
    }

    void shutdown() {
        this.retryExecutor.shutdownNow();
        this.callbackExecutor.shutdownNow();
        this.httpClient.shutdown();
    }
}
