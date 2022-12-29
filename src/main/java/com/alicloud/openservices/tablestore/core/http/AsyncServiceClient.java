package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.alicloud.openservices.tablestore.RequestTracer;
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

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

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
            PoolingNHttpClientConnectionManager cm =
                    new PoolingNHttpClientConnectionManager(ioReactor);
            cm.setMaxTotal(config.getMaxConnections());
            cm.setDefaultMaxPerRoute(config.getMaxConnections());
            httpClient = HttpFactory.createHttpAsyncClient(config, cm);

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
