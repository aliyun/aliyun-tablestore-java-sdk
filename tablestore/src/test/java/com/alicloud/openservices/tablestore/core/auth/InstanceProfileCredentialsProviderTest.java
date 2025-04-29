package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.ClientException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class InstanceProfileCredentialsProviderTest {

    private volatile String responseJson = "";
    private AtomicLong accessCount = new AtomicLong(0);
    private AtomicBoolean failInjection = new AtomicBoolean(false);
    private volatile HttpServer server;

    @Before
    public void init() throws IOException {
        failInjection.set(false);
        accessCount.set(0);
        responseJson = "";
        startMockServer();
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.stop(5);
        }
    }

    private void startMockServer() throws IOException  {
        accessCount.set(0);
        server = HttpServer.create(new InetSocketAddress(9999), 0);
        server.createContext("/refreshToken/", new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                accessCount.incrementAndGet();
                if (!failInjection.get()) {
                    refreshCredentials(accessCount.get());
                }
                byte[] bs = responseJson.getBytes();
                httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, bs.length);
                httpExchange.getResponseBody().write(bs);
                httpExchange.close();
            }
        });
        server.start();
    }

    class InstanceProfileCredentialsFetcherForTest extends InstanceProfileCredentialsFetcher {
        @Override
        public URL buildUrl() throws ClientException {
            try {
                return new URL("http://127.0.0.1:9999/refreshToken/");
            } catch (MalformedURLException e) {
                throw new ClientException(e);
            }
        }

        @Override
        public ServiceCredentials fetch() throws ClientException {
            InstanceProfileCredentials ipc = (InstanceProfileCredentials) super.fetch();
            ipc.withRefreshIntervalInMilliseconds(1000).withExpiredDuration(5).withExpiredFactor(0.6);
            return ipc;
        }
    }

    private void setResponse(String code, String accessId, String accessKey, String token, String expiration) {
        this.responseJson = String.format("{\"Code\":\"%s\",\"AccessKeyId\":\"%s\",\"AccessKeySecret\":\"%s\",\"SecurityToken\":\"%s\",\"Expiration\":\"%s\"}",
                code, accessId, accessKey, token, expiration);
    }

    @Test
    public void testHttpCredentialsFetcher() throws Exception {
        InstanceProfileCredentialsFetcherForTest fetcher = new InstanceProfileCredentialsFetcherForTest();

        // invalid response
        try {
            failInjection.set(true);
            fetcher.fetch(3);
            fail("expect failure.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(accessCount.get(), 3);

        setResponse("Success", "accessid", "accesskey", "token", "2018-12-25T00:00:00Z");
        ServiceCredentials credentials = fetcher.fetch();
        assertEquals(accessCount.get(), 4);

        assertEquals(credentials.getAccessKeyId(), "accessid");
        assertEquals(credentials.getAccessKeySecret(), "accesskey");
        assertEquals(credentials.getSecurityToken(), "token");
    }

    @Test
    public void testInstanceProfileCredentialsProvider() throws Exception {
        final InstanceProfileCredentialsProvider cp = CredentialsProviderFactory.newInstanceProfileCredentialsProvider("role");
        InstanceProfileCredentialsFetcherForTest fetcher = new InstanceProfileCredentialsFetcherForTest();
        cp.withCredentialsFetcher(fetcher);

        final AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> threadsTryGetCreds = new ArrayList<Thread>();
        for (int i = 0; i < 1; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        InstanceProfileCredentials cd = cp.getCredentials();
                    }
                }
            });
            threadsTryGetCreds.add(t);
        }

        for (Thread t : threadsTryGetCreds) {
            t.start();
        }

        Thread.sleep(13000);

        stop.set(true);
        for (Thread t : threadsTryGetCreds) {
            t.join();
        }

        // refresh every 3s
        assertEquals(accessCount.get(), 5);
    }

    private void refreshCredentials(long i) {
        Date d = new Date(System.currentTimeMillis() + 5000); // 5s duration
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        parser.setTimeZone(TimeZone.getTimeZone("GMT"));

        setResponse("Success", "accessid" + i, "accesskey" + i, "token" + i, parser.format(d));
    }
}
