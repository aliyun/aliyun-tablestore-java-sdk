package com.alicloud.openservices.tablestore.core.memory;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.LauncherFactory;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.NoRetryStrategy;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.memory.MemoryRequest;
import com.alicloud.openservices.tablestore.model.memory.SearchMemoriesRequest;
import com.alicloud.openservices.tablestore.model.memory.SearchMemoriesResponse;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JsonOperationLauncherTest {

    @Test
    public void factoryCreatesTypedLauncherForRequestAction() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        AsyncServiceClient client = new AsyncServiceClient(config);
        try {
            CredentialsProvider credentials = CredentialsProviderFactory.newDefaultCredentialProvider("ak", "secret", null);
            LauncherFactory factory = new LauncherFactory("https://example.com", "instance", client, credentials, config);
            SearchMemoriesRequest request = new SearchMemoriesRequest("store", "query");

            JsonOperationLauncher<SearchMemoriesRequest, SearchMemoriesResponse> launcher =
                    factory.memoryOperation(new TraceLogger("trace", 1000), new NoRetryStrategy(),
                            request, SearchMemoriesResponse.class);

            assertEquals("SearchMemories", launcher.getUri().getAction());
            assertEquals(SearchMemoriesResponse.class, launcher.getResponseClass());
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void redactsMemoryPayloadFromDebugLog() throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        AsyncServiceClient client = new AsyncServiceClient(config);
        try {
            CredentialsProvider credentials = CredentialsProviderFactory.newDefaultCredentialProvider("ak", "secret", null);
            LauncherFactory factory = new LauncherFactory("https://example.com", "instance", client, credentials, config);
            SearchMemoriesRequest request = new SearchMemoriesRequest("store", "private-memory-content");
            JsonOperationLauncher<SearchMemoriesRequest, SearchMemoriesResponse> launcher =
                    factory.memoryOperation(new TraceLogger("trace", 1000), new NoRetryStrategy(),
                            request, SearchMemoriesResponse.class);

            String formatted = launcher.formatRequestMessageForLog(request.toJson());

            assertFalse(formatted.contains("private-memory-content"));
            assertTrue(formatted.contains("redacted"));
            assertTrue(formatted.contains("bytes"));
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void factoryRegistersEveryMemoryAction() throws Exception {
        List<String> actions = Arrays.asList(
                OperationNames.OP_CREATE_MEMORY_STORE,
                OperationNames.OP_GET_MEMORY_STORE,
                OperationNames.OP_LIST_MEMORY_STORES,
                OperationNames.OP_UPDATE_MEMORY_STORE,
                OperationNames.OP_DELETE_MEMORY_STORE,
                OperationNames.OP_ADD_MEMORIES,
                OperationNames.OP_SEARCH_MEMORIES,
                OperationNames.OP_LIST_MEMORIES,
                OperationNames.OP_GET_MEMORY,
                OperationNames.OP_UPDATE_MEMORY,
                OperationNames.OP_DELETE_MEMORY,
                OperationNames.OP_LIST_MEMORY_STORE_MESSAGES,
                OperationNames.OP_LIST_MEMORY_STORE_REQUESTS,
                OperationNames.OP_GET_MEMORY_TASK,
                OperationNames.OP_LIST_MEMORY_TASKS,
                OperationNames.OP_LIST_MEMORY_STORE_SCOPES,
                OperationNames.OP_CREATE_MEMORY_DREAM_TASK,
                OperationNames.OP_GET_MEMORY_DREAM_TASK,
                OperationNames.OP_LIST_MEMORY_DREAM_TASKS,
                OperationNames.OP_CANCEL_MEMORY_DREAM_TASK,
                OperationNames.OP_LIST_MEMORY_DREAM_ACTIONS,
                OperationNames.OP_APPLY_MEMORY_DREAM_ACTIONS,
                OperationNames.OP_ADD_ITEM,
                OperationNames.OP_LIST_ITEMS,
                OperationNames.OP_GET_ITEM,
                OperationNames.OP_UPDATE_ITEM,
                OperationNames.OP_DELETE_ITEM,
                OperationNames.OP_LIST_ITEM_VERSIONS,
                OperationNames.OP_GET_ITEM_VERSION,
                OperationNames.OP_REDACT_ITEM_VERSION);
        ClientConfiguration config = new ClientConfiguration();
        AsyncServiceClient client = new AsyncServiceClient(config);
        try {
            CredentialsProvider credentials = CredentialsProviderFactory.newDefaultCredentialProvider("ak", "secret", null);
            LauncherFactory factory = new LauncherFactory("https://example.com", "instance", client, credentials, config);
            for (String action : actions) {
                TestMemoryRequest request = new TestMemoryRequest(action);
                JsonOperationLauncher<TestMemoryRequest, Response> launcher =
                        factory.memoryOperation(new TraceLogger("trace", 1000), new NoRetryStrategy(),
                                request, Response.class);
                assertEquals(action, launcher.getUri().getAction());
            }
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void retryClassificationMatchesMemoryOperationSemantics() {
        List<String> idempotentActions = Arrays.asList(
                OperationNames.OP_GET_MEMORY_STORE,
                OperationNames.OP_LIST_MEMORY_STORES,
                OperationNames.OP_DELETE_MEMORY_STORE,
                OperationNames.OP_SEARCH_MEMORIES,
                OperationNames.OP_LIST_MEMORIES,
                OperationNames.OP_GET_MEMORY,
                OperationNames.OP_DELETE_MEMORY,
                OperationNames.OP_LIST_MEMORY_STORE_MESSAGES,
                OperationNames.OP_LIST_MEMORY_STORE_REQUESTS,
                OperationNames.OP_GET_MEMORY_TASK,
                OperationNames.OP_LIST_MEMORY_TASKS,
                OperationNames.OP_LIST_MEMORY_STORE_SCOPES,
                OperationNames.OP_GET_MEMORY_DREAM_TASK,
                OperationNames.OP_LIST_MEMORY_DREAM_TASKS,
                OperationNames.OP_CANCEL_MEMORY_DREAM_TASK,
                OperationNames.OP_LIST_MEMORY_DREAM_ACTIONS,
                OperationNames.OP_LIST_ITEMS,
                OperationNames.OP_GET_ITEM,
                OperationNames.OP_DELETE_ITEM,
                OperationNames.OP_LIST_ITEM_VERSIONS,
                OperationNames.OP_GET_ITEM_VERSION,
                OperationNames.OP_REDACT_ITEM_VERSION);
        List<String> nonIdempotentActions = Arrays.asList(
                OperationNames.OP_CREATE_MEMORY_STORE,
                OperationNames.OP_UPDATE_MEMORY_STORE,
                OperationNames.OP_ADD_MEMORIES,
                OperationNames.OP_UPDATE_MEMORY,
                OperationNames.OP_CREATE_MEMORY_DREAM_TASK,
                OperationNames.OP_APPLY_MEMORY_DREAM_ACTIONS,
                OperationNames.OP_ADD_ITEM,
                OperationNames.OP_UPDATE_ITEM);

        for (String action : idempotentActions) {
            assertTrue(action, OperationNames.IdempotentActionTool.isIdempotentAction(action));
        }
        for (String action : nonIdempotentActions) {
            assertFalse(action, OperationNames.IdempotentActionTool.isIdempotentAction(action));
        }
    }

    private static class TestMemoryRequest implements MemoryRequest {
        private final String operationName;

        private TestMemoryRequest(String operationName) {
            this.operationName = operationName;
        }

        @Override
        public String getOperationName() {
            return operationName;
        }

        @Override
        public String toJson() {
            return "{}";
        }
    }
}
