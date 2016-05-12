/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.comm;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.internal.OTSAsyncResponseConsumer;
import com.aliyun.openservices.ots.internal.OTSTraceLogger;
import com.aliyun.openservices.ots.log.LogUtil;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.aliyun.openservices.ots.internal.OTSLoggerConstant.*;

public class AsyncServiceClient extends ServiceClient {
    private CloseableHttpAsyncClient httpClient;
    private IdleConnectionEvictor connEvictor;
    
    public AsyncServiceClient(ClientConfiguration config) {
        super(config);
        try {
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getIoThreadCount()).build();
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(
                    ioReactorConfig);
            PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(
                    ioReactor);
            cm.setMaxTotal(config.getMaxConnections());
            cm.setDefaultMaxPerRoute(config.getMaxConnections());
            httpClient = new HttpFactory().createHttpAsyncClient(config, cm);

            /*
             * socketTimeout的值限制了closeIdleConnections执行的周期。
             * 如果周期相对socketTimeout的值过长，有可能一个请求分配到一个即将socketTimeout的连接，
             * 在请求发送之前即抛出SocketTimeoutException。
             * 现在让closeIdleConnections的执行周期为socketTimeout / 2.5。
             */
            long closePeriod = 5000;
            if (config.getSocketTimeoutInMillisecond() > 0) {
                closePeriod = (long) (config.getSocketTimeoutInMillisecond() / 2.5);
            }
            closePeriod = closePeriod < 5000 ? closePeriod : 5000;
            connEvictor = new IdleConnectionEvictor(cm, closePeriod);
            httpClient.start();
            connEvictor.start();
        } catch (IOReactorException ex) {
            throw new ClientException(String.format("IOReactorError: %s",
                    ex.getMessage()), ex);
        }
    }

    public static class IdleConnectionEvictor extends Thread {
        private final NHttpClientConnectionManager connMgr;
        private volatile boolean shutdown;
        private long closePeriod;

        public IdleConnectionEvictor(NHttpClientConnectionManager connMgr,
                long closePeriod) {
            this.connMgr = connMgr;
            this.closePeriod = closePeriod;
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    synchronized (this) {
                        wait(closePeriod);
                        connMgr.closeExpiredConnections();
                        connMgr.closeIdleConnections(closePeriod,
                                TimeUnit.MILLISECONDS);
                    }
                }
            } catch (InterruptedException ex) {
                // terminate
            }
        }

        public void shutdown() {
            shutdown = true;
            synchronized (this) {
                notifyAll();
            }
        }
    }

    static class OTSRequestProducer extends BasicAsyncRequestProducer {
        private OTSTraceLogger traceLogger;
        public OTSRequestProducer(final HttpHost target,
                final HttpRequest request, OTSTraceLogger traceLogger) {
            super(target, request);
            this.traceLogger = traceLogger;
        }

        public void requestCompleted(final HttpContext context) {
            super.requestCompleted(context);
            if (LogUtil.LOG.isDebugEnabled()) {
                LogUtil.LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId() + DELIMITER + REQUEST_SENT);
            }
            traceLogger.addEventTime(REQUEST_SENT, System.currentTimeMillis());
        }
    }

    @Override
    protected <Res> void asyncSendRequestCore(RequestMessage request,
            ExecutionContext context, OTSAsyncResponseConsumer<Res> consumer,
            FutureCallback<Res> callback, OTSTraceLogger traceLogger) {
        final HttpHost target = request.getActionUri().getHost();
        if (LogUtil.LOG.isDebugEnabled()) {
            LogUtil.LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId() + DELIMITER + INTO_HTTP_ASYNC_CLIENT);
        }
        traceLogger.addEventTime(INTO_HTTP_ASYNC_CLIENT, System.currentTimeMillis());
        httpClient.execute(new OTSRequestProducer(target, request.getRequest(), traceLogger),
                consumer, callback);
    }

    @Override
    public void shutdown() {
        try {
            this.connEvictor.shutdown();
            this.connEvictor.join();
            this.httpClient.close();
        } catch (IOException e) {
            throw new ClientException(rm.getFormattedString("IOError",
                    e.getMessage()), e);
        } catch (InterruptedException e) {

        }
    }
}
