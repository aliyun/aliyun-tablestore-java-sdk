package com.alicloud.openservices.tablestore.core.memory;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.NoRetryStrategy;
import com.alicloud.openservices.tablestore.model.memory.AddItemRequest;
import com.alicloud.openservices.tablestore.model.memory.AddItemResponse;
import com.alicloud.openservices.tablestore.model.memory.Scope;
import com.alicloud.openservices.tablestore.model.memory.SearchMemoriesRequest;
import com.alicloud.openservices.tablestore.model.memory.SearchMemoriesResponse;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MemoryJsonOperationTransportTest {
    private static final Scope SCOPE = new Scope("app", "tenant", "agent", "run");

    @Test
    public void sendsMemoryJsonPostAndCompletesFutureAndCallback() throws Exception {
        ResponseSpec response = ResponseSpec.success(
                "transport-search-1", "{\"memoryStoreName\":\"store\",\"results\":[]}");
        try (TestServer server = new TestServer(response)) {
            AsyncClient client = newClient(server, false);
            try {
                SearchMemoriesRequest request = new SearchMemoriesRequest("store", "remember this");
                request.setScope(SCOPE);
                request.setEnableRerank(false);
                AtomicReference<SearchMemoriesResponse> callbackResponse = new AtomicReference<SearchMemoriesResponse>();
                AtomicReference<Exception> callbackFailure = new AtomicReference<Exception>();
                CountDownLatch callbackLatch = new CountDownLatch(1);

                Future<SearchMemoriesResponse> future = client.searchMemories(request,
                        new TableStoreCallback<SearchMemoriesRequest, SearchMemoriesResponse>() {
                            @Override
                            public void onCompleted(SearchMemoriesRequest req, SearchMemoriesResponse res) {
                                callbackResponse.set(res);
                                callbackLatch.countDown();
                            }

                            @Override
                            public void onFailed(SearchMemoriesRequest req, Exception ex) {
                                callbackFailure.set(ex);
                                callbackLatch.countDown();
                            }
                        });

                SearchMemoriesResponse result = future.get(5, TimeUnit.SECONDS);
                assertTrue("callback did not complete", callbackLatch.await(5, TimeUnit.SECONDS));
                assertEquals(result, callbackResponse.get());
                assertEquals(null, callbackFailure.get());
                assertEquals("transport-search-1", result.getRequestId());
                assertEquals("store", result.getMemoryStoreName());

                assertEquals("POST", server.getMethods().get(0));
                assertEquals("/SearchMemories", server.getPaths().get(0));
                JsonObject body = new JsonParser().parse(server.getBodies().get(0)).getAsJsonObject();
                assertEquals("store", body.get("memoryStoreName").getAsString());
                assertEquals("remember this", body.get("query").getAsString());
                assertFalse(body.get("enableRerank").getAsBoolean());
                assertEquals("run", body.getAsJsonObject("scope").get("runId").getAsString());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    public void sendsItemJsonWithDefaultType() throws Exception {
        ResponseSpec response = ResponseSpec.success("transport-item-1",
                "{\"type\":\"memoryfile\",\"itemId\":\"item-1\",\"path\":\"/a.md\"," +
                        "\"content\":\"\",\"contentSha256\":\"sha\",\"contentSizeBytes\":0," +
                        "\"latestSeq\":1,\"createdAt\":\"c\",\"updatedAt\":\"u\"}");
        try (TestServer server = new TestServer(response)) {
            AsyncClient client = newClient(server, false);
            try {
                AddItemResponse result = client.addItem(
                        new AddItemRequest("store", SCOPE, "/a.md", ""), null).get(5, TimeUnit.SECONDS);

                assertEquals("item-1", result.getItemId());
                assertEquals("memoryfile", result.getType());
                assertEquals("/AddItem", server.getPaths().get(0));
                JsonObject body = new JsonParser().parse(server.getBodies().get(0)).getAsJsonObject();
                assertEquals("memoryfile", body.get("type").getAsString());
                assertEquals("", body.get("content").getAsString());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    public void mapsJsonServiceErrors() throws Exception {
        ResponseSpec response = ResponseSpec.error(400, "service-request-1",
                "{\"code\":\"InvalidParameter\",\"message\":\"bad memory request\"}");
        try (TestServer server = new TestServer(response)) {
            AsyncClient client = newClient(server, false);
            try {
                Exception failure = getFailure(client.searchMemories(
                        new SearchMemoriesRequest("store", "query"), null));

                assertTrue(failure instanceof TableStoreException);
                TableStoreException serviceException = (TableStoreException) failure;
                assertEquals("InvalidParameter", serviceException.getErrorCode());
                assertEquals("bad memory request", serviceException.getMessage());
                assertEquals("service-request-1", serviceException.getRequestId());
                assertEquals(400, serviceException.getHttpStatus());
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    public void rejectsMalformedJsonAndMissingRequestId() throws Exception {
        try (TestServer malformedServer = new TestServer(
                ResponseSpec.success("transport-bad-json", "{"))) {
            AsyncClient client = newClient(malformedServer, false);
            try {
                Exception failure = getFailure(client.searchMemories(
                        new SearchMemoriesRequest("store", "query"), null));
                assertTrue(failure instanceof ClientException);
                assertTrue(failure.getMessage(), failure.getMessage().contains("parse JSON response"));
            } finally {
                client.shutdown();
            }
        }

        try (TestServer missingHeaderServer = new TestServer(
                new ResponseSpec(200, null, "{\"memoryStoreName\":\"store\",\"results\":[]}"))) {
            AsyncClient client = newClient(missingHeaderServer, false);
            try {
                Exception failure = getFailure(client.searchMemories(
                        new SearchMemoriesRequest("store", "query"), null));
                assertTrue(failure instanceof ClientException);
                assertTrue(failure.getMessage(), failure.getMessage().contains("required header is missing"));
            } finally {
                client.shutdown();
            }
        }
    }

    @Test
    public void retriesIdempotentMemoryReadsThroughTheTransport() throws Exception {
        ResponseSpec first = ResponseSpec.error(500, "retry-request-1",
                "{\"code\":\"InternalServerError\",\"message\":\"try again\"}");
        ResponseSpec second = ResponseSpec.success(
                "retry-request-2", "{\"memoryStoreName\":\"store\",\"results\":[]}");
        try (TestServer server = new TestServer(first, second)) {
            AsyncClient client = newClient(server, true);
            try {
                SearchMemoriesResponse result = client.searchMemories(
                        new SearchMemoriesRequest("store", "query"), null).get(5, TimeUnit.SECONDS);

                assertEquals("retry-request-2", result.getRequestId());
                assertEquals(2, server.getPaths().size());
                assertEquals("/SearchMemories", server.getPaths().get(0));
                assertEquals("/SearchMemories", server.getPaths().get(1));
                assertEquals(server.getBodies().get(0), server.getBodies().get(1));
            } finally {
                client.shutdown();
            }
        }
    }

    private static AsyncClient newClient(TestServer server, boolean enableRetries) {
        ClientConfiguration config = new ClientConfiguration();
        config.setConnectionTimeoutInMillisecond(1000);
        config.setSocketTimeoutInMillisecond(1000);
        config.setEnableResponseValidation(false);
        if (!enableRetries) {
            config.setRetryStrategy(new NoRetryStrategy());
        }
        return new AsyncClient(server.getEndpoint(), "access-key", "secret", "instance", config);
    }

    private static Exception getFailure(Future<?> future) throws Exception {
        try {
            future.get(5, TimeUnit.SECONDS);
            fail("request unexpectedly succeeded");
            return null;
        } catch (TableStoreException e) {
            return e;
        } catch (ClientException e) {
            return e;
        } catch (ExecutionException e) {
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof Exception);
            return (Exception) e.getCause();
        }
    }

    private static final class ResponseSpec {
        private final int status;
        private final String requestId;
        private final String body;

        private ResponseSpec(int status, String requestId, String body) {
            this.status = status;
            this.requestId = requestId;
            this.body = body;
        }

        private static ResponseSpec success(String requestId, String body) {
            return new ResponseSpec(200, requestId, body);
        }

        private static ResponseSpec error(int status, String requestId, String body) {
            return new ResponseSpec(status, requestId, body);
        }
    }

    private static final class TestServer implements AutoCloseable, HttpHandler {
        private final HttpServer server;
        private final Queue<ResponseSpec> responses = new ConcurrentLinkedQueue<ResponseSpec>();
        private final List<String> methods = Collections.synchronizedList(new ArrayList<String>());
        private final List<String> paths = Collections.synchronizedList(new ArrayList<String>());
        private final List<String> bodies = Collections.synchronizedList(new ArrayList<String>());

        private TestServer(ResponseSpec... responseSpecs) throws IOException {
            Collections.addAll(responses, responseSpecs);
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            server.createContext("/", this);
            server.start();
        }

        private String getEndpoint() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        private List<String> getMethods() {
            return methods;
        }

        private List<String> getPaths() {
            return paths;
        }

        private List<String> getBodies() {
            return bodies;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            methods.add(exchange.getRequestMethod());
            paths.add(exchange.getRequestURI().getPath());
            bodies.add(readUtf8(exchange.getRequestBody()));

            ResponseSpec response = responses.poll();
            if (response == null) {
                response = ResponseSpec.error(500, "unexpected-request", "{\"message\":\"unexpected request\"}");
            }
            byte[] body = response.body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            if (response.requestId != null) {
                exchange.getResponseHeaders().set("x-ots-requestid", response.requestId);
            }
            exchange.sendResponseHeaders(response.status, body.length);
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static String readUtf8(InputStream input) throws IOException {
        try (InputStream source = input; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int count;
            while ((count = source.read(buffer)) != -1) {
                output.write(buffer, 0, count);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
