package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.RequestTracer;
import com.aliyun.ots.thirdparty.org.apache.http.HttpHost;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;
import com.aliyun.ots.thirdparty.org.apache.http.config.Registry;
import com.aliyun.ots.thirdparty.org.apache.http.config.RegistryBuilder;
import com.aliyun.ots.thirdparty.org.apache.http.conn.DnsResolver;
import com.aliyun.ots.thirdparty.org.apache.http.conn.ssl.SSLContexts;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.reactor.IOReactorConfig;
import com.aliyun.ots.thirdparty.org.apache.http.nio.conn.NHttpClientConnectionManager;
import com.aliyun.ots.thirdparty.org.apache.http.nio.conn.NoopIOSessionStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.nio.conn.SchemeIOSessionStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import com.aliyun.ots.thirdparty.org.apache.http.nio.reactor.ConnectingIOReactor;
import com.aliyun.ots.thirdparty.org.apache.http.nio.reactor.IOReactorException;
import com.aliyun.ots.thirdparty.org.apache.http.protocol.HttpContext;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import javax.net.ssl.SSLContext;

import static com.alicloud.openservices.tablestore.core.utils.LogUtil.*;

public class AsyncServiceClient {
    private CloseableHttpAsyncClient httpClient;
    private IdleConnectionEvictor connEvictor;
    private Map<String, String> extraHeaders;

    public AsyncServiceClient(ClientConfiguration config) {
        try {
            IOReactorConfig ioReactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getIoThreadCount()).build();
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(
                    ioReactorConfig);

            SSLContext sslContext = SSLContexts.createDefault();
            if (config.getSslSessionCacheSize() >= 0) {
                sslContext.getClientSessionContext().setSessionCacheSize(config.getSslSessionCacheSize());
            }
            if (config.getSslSessionTimeoutInSec() >= 0) {
                sslContext.getClientSessionContext().setSessionTimeout(config.getSslSessionTimeoutInSec());
            }
            SSLIOSessionStrategy sslioSessionStrategy = new SSLIOSessionStrategy(sslContext, SSLIOSessionStrategy.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);

            Registry<SchemeIOSessionStrategy> iosessionFactoryRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                    .register("http", NoopIOSessionStrategy.INSTANCE)
                    .register("https", sslioSessionStrategy)
                    .build();
            DnsResolver dnsResolver = null;
            if (config.isEnableDnsCache()) {
                dnsResolver = new CachedDnsResolver(config.getDnsCacheMaxSize(), config.getDnsCacheExpireAfterWriteSec(), config.getDnsCacheRefreshAfterWriteSec());
            }
            PoolingNHttpClientConnectionManager cm =
                    new PoolingNHttpClientConnectionManager(ioReactor, null, iosessionFactoryRegistry, dnsResolver);
            cm.setMaxTotal(config.getMaxConnections());
            cm.setDefaultMaxPerRoute(config.getMaxConnections());
            httpClient = HttpFactory.createHttpAsyncClient(config, cm);

            /*
             * The value of socketTimeout limits the execution period of closeIdleConnections.
             * If the period is too long relative to the value of socketTimeout, there is a possibility that 
             * a request is assigned to a connection that is about to reach socketTimeout, 
             * causing a SocketTimeoutException to be thrown before the request is sent. 
             * We now set the execution period of closeIdleConnections to socketTimeout / 2.5.
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

    public Map<String, String> getExtraHeaders() {
        return extraHeaders;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.extraHeaders = extraHeaders;
    }

    static class IdleConnectionEvictor extends Thread {
        private final NHttpClientConnectionManager connMgr;
        private volatile boolean shutdown;
        private long closePeriod;

        public IdleConnectionEvictor(
                NHttpClientConnectionManager connMgr,
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
                        connMgr.closeIdleConnections(
                                closePeriod, TimeUnit.MILLISECONDS);
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
        private TraceLogger traceLogger;
        private RequestMessage requestMessage;
        private Object rpcContext;
        private RequestTracer requestTracer;

        public OTSRequestProducer(
                final HttpHost target,
                final RequestMessage request,
                TraceLogger traceLogger,
                RequestTracer requestTracer,
                Object rpcContext) {
            super(target, request.getRequest());
            this.traceLogger = traceLogger;
            this.requestMessage = request;
            this.requestTracer = requestTracer;
            this.rpcContext = rpcContext;
        }

        public void requestCompleted(final HttpContext context) {
            super.requestCompleted(context);
            if (LOG.isDebugEnabled()) {
                LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId() + DELIMITER + REQUEST_SENT);
            }
            if (requestTracer != null) {
                RequestTracer.RequestSendTraceInfo requestSendTraceInfo =
                        new RequestTracer.RequestSendTraceInfo(
                                requestMessage.getContentLength(),
                                requestMessage.getActionUri().getHost().getHostName(),
                                rpcContext);
                requestTracer.requestSend(requestSendTraceInfo);
            }
            traceLogger.addEventTime(REQUEST_SENT, System.currentTimeMillis());
        }
    }

    public <Res> void asyncSendRequest(
            RequestMessage request,
            ExecutionContext context,
            ResponseConsumer<Res> consumer,
            FutureCallback<Res> callback,
            TraceLogger traceLogger,
            RequestTracer requestTracer,
            Object rpcContext) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(context);

        addExtraHeaders(request);

        context.getSigner().sign(request);
        handleRequest(request, context.getResquestHandlers());
        consumer.setContext(context);
        final HttpHost target = request.getActionUri().getHost();
        if (LOG.isDebugEnabled()) {
            LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId() + DELIMITER + INTO_HTTP_ASYNC_CLIENT);
        }
        traceLogger.addEventTime(INTO_HTTP_ASYNC_CLIENT, System.currentTimeMillis());
        httpClient.execute(
                new OTSRequestProducer(target, request, traceLogger, requestTracer, rpcContext),
                consumer, callback);
    }

    private void addExtraHeaders(RequestMessage request) {
        if (extraHeaders == null) {
            return;
        }
        for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
            request.addHeader(entry.getKey(), entry.getValue());
        }
    }

    private void handleRequest(
            RequestMessage message,
            List<RequestHandler> requestHandlers)
            throws ClientException {
        for (RequestHandler h : requestHandlers) {
            h.handle(message);
        }
    }

    public void shutdown() {
        try {
            this.connEvictor.shutdown();
            this.connEvictor.join();
            this.httpClient.close();
        } catch (IOException e) {
            throw new ClientException("Failed to shutdown http client.", e);
        } catch (InterruptedException e) {
            // ignore exception
        }
    }
}
