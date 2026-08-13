package com.alicloud.openservices.tablestore.model.memory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class MemoryRequestSerializationTest {

    private static final Scope SCOPE = new Scope("app", "tenant", "agent", "run");

    private JsonObject json(MemoryRequest request) {
        return new JsonParser().parse(request.toJson()).getAsJsonObject();
    }

    @Test
    public void serializesStoreAndPatchRequests() {
        CreateMemoryStoreRequest create = new CreateMemoryStoreRequest("store_a");
        create.setDescription("store");
        create.setExtractInstructions("remember preferences");
        create.setStorageMode("filemem");
        JsonObject createJson = json(create);
        assertEquals("store_a", createJson.get("memoryStoreName").getAsString());
        assertEquals("filemem", createJson.get("storageMode").getAsString());

        UpdateMemoryStoreRequest update = new UpdateMemoryStoreRequest("store_a");
        update.setDescription("");
        JsonObject updateJson = json(update);
        assertTrue(updateJson.has("description"));
        assertEquals("", updateJson.get("description").getAsString());
        assertFalse(updateJson.has("extractInstructions"));
    }

    @Test
    public void preservesNestedValuesFalseAndZero() {
        AddMemoriesRequest add = new AddMemoriesRequest("store_a");
        add.setScope(SCOPE);
        add.setMessages(Collections.singletonList(new MessagePayload("user", "hello")));
        add.setSync(false);
        JsonObject addJson = json(add);
        assertFalse(addJson.get("sync").getAsBoolean());
        assertEquals("hello", addJson.getAsJsonArray("messages").get(0).getAsJsonObject().get("content").getAsString());

        SearchMemoriesRequest search = new SearchMemoriesRequest("store_a", "hello");
        search.setScope(SCOPE);
        search.setContextScope(SCOPE);
        search.setEnableRerank(false);
        search.setMinSimilarity(0D);
        search.setIncludeEvidence(false);
        JsonObject searchJson = json(search);
        assertFalse(searchJson.get("enableRerank").getAsBoolean());
        assertEquals(0D, searchJson.get("minSimilarity").getAsDouble(), 0D);
        assertFalse(searchJson.get("includeEvidence").getAsBoolean());
        assertEquals("run", searchJson.getAsJsonObject("contextScope").get("runId").getAsString());
    }

    @Test
    public void scopedConvenienceConstructorsPopulateAllRequiredIdentifiers() {
        GetMemoryTaskRequest task = new GetMemoryTaskRequest("store_a", "request-1", SCOPE);
        assertEquals("store_a", task.getMemoryStoreName());
        assertEquals("request-1", task.getRequestId());
        assertSame(SCOPE, task.getScope());

        ListMemoriesRequest list = new ListMemoriesRequest("store_a", SCOPE);
        assertEquals("store_a", list.getMemoryStoreName());
        assertSame(SCOPE, list.getScope());
    }

    @Test
    public void preservesTimestampJsonKindsAndDreamOptions() {
        ListMemoryStoreMessagesRequest messages = new ListMemoryStoreMessagesRequest("store_a", SCOPE);
        messages.setMinTimestamp("2026-08-12T00:00:00Z");
        messages.setMaxTimestamp(1770000000000L);
        JsonObject messageJson = json(messages);
        assertTrue(messageJson.get("minTimestamp").isJsonPrimitive());
        assertEquals("2026-08-12T00:00:00Z", messageJson.get("minTimestamp").getAsString());
        assertEquals(1770000000000L, messageJson.get("maxTimestamp").getAsLong());

        CreateMemoryDreamTaskRequest dream = new CreateMemoryDreamTaskRequest("store_a", Collections.singletonList(SCOPE));
        dream.setTaskType("skill");
        dream.setIncremental(false);
        dream.setMaxMemories(0);
        dream.setConfidenceThresholds(Collections.singletonMap("add", 0.8D));
        JsonObject dreamJson = json(dream);
        assertEquals("skill", dreamJson.get("taskType").getAsString());
        assertFalse(dreamJson.get("incremental").getAsBoolean());
        assertEquals(0, dreamJson.get("maxMemories").getAsInt());
        assertEquals(0.8D, dreamJson.getAsJsonObject("confidenceThresholds").get("add").getAsDouble(), 0D);
    }

    @Test
    public void timestampSettersAcceptOnlyPrimitiveLongsAndStrings() throws Exception {
        Class<?>[] requestTypes = {
                CreateMemoryDreamTaskRequest.class,
                ListMemoryDreamTasksRequest.class,
                ListMemoryTasksRequest.class,
                ListMemoryStoreMessagesRequest.class,
                ListMemoryStoreRequestsRequest.class
        };

        for (Class<?> requestType : requestTypes) {
            assertTimestampSetter(requestType, "setMinTimestamp");
            assertTimestampSetter(requestType, "setMaxTimestamp");
        }
    }

    private static void assertTimestampSetter(Class<?> requestType, String methodName) throws Exception {
        Method primitiveSetter = requestType.getMethod(methodName, long.class);
        Method stringSetter = requestType.getMethod(methodName, String.class);
        assertEquals(void.class, primitiveSetter.getReturnType());
        assertEquals(void.class, stringSetter.getReturnType());
        try {
            requestType.getMethod(methodName, Long.class);
            fail(requestType.getSimpleName() + "." + methodName + " must not make null calls ambiguous");
        } catch (NoSuchMethodException expected) {
            // Primitive long and String match the number|string wire contract without null ambiguity.
        }
        try {
            requestType.getMethod(methodName, Object.class);
            fail(requestType.getSimpleName() + "." + methodName + " must not accept Object");
        } catch (NoSuchMethodException expected) {
            // The wire contract accepts only JSON numbers and strings.
        }
    }

    @Test
    public void exposesMemoryAndDreamWireConstants() {
        assertEquals("queued", MemoryConstants.MEMORY_TASK_STATUS_QUEUED);
        assertEquals("needs_reconcile", MemoryConstants.MEMORY_TASK_STATUS_NEEDS_RECONCILE);
        assertEquals("completed_with_failures", MemoryConstants.DREAM_TASK_STATUS_COMPLETED_WITH_FAILURES);
        assertEquals("safe_auto", MemoryConstants.DREAM_APPLY_MODE_SAFE_AUTO);
        assertEquals("promote_scope", MemoryConstants.DREAM_SCOPE_OUTPUT_MODE_PROMOTE_SCOPE);
        assertEquals("EMIT_SKILL", MemoryConstants.DREAM_ACTION_EMIT_SKILL);
        assertEquals("confidence_below_threshold", MemoryConstants.DREAM_SKIPPED_CONFIDENCE_BELOW_THRESHOLD);
        assertEquals("confidence_desc", MemoryConstants.DREAM_ACTION_ORDER_CONFIDENCE_DESC);
        assertEquals("not_found", MemoryConstants.DREAM_ACTION_STATUS_NOT_FOUND);
    }

    @Test
    public void rawJsonOverrideIsSentVerbatim() {
        CreateMemoryStoreRequest request = new CreateMemoryStoreRequest("ignored");
        request.setJsonStr("{\"futureField\":true}");
        assertEquals("{\"futureField\":true}", request.toJson());
    }

    @Test
    public void itemRequestsInjectDefaultTypeAndPreserveCustomType() {
        MemoryRequest[] requests = new MemoryRequest[] {
                new AddItemRequest("store", SCOPE, "/a.md", ""),
                new ListItemsRequest("store", SCOPE),
                new GetItemRequest("store", SCOPE, "/a.md"),
                new UpdateItemRequest("store", SCOPE, "/a.md"),
                new DeleteItemRequest("store", SCOPE, "/a.md"),
                new ListItemVersionsRequest("store", SCOPE, "item-1"),
                new GetItemVersionRequest("store", SCOPE, "item-1", "version-1", 1L),
                new RedactItemVersionRequest("store", SCOPE, "item-1", "version-1", 1L)
        };
        for (MemoryRequest request : requests) {
            assertEquals("memoryfile", json(request).get("type").getAsString());
        }
        ((GetItemRequest) requests[2]).setType("customtype");
        assertEquals("customtype", json(requests[2]).get("type").getAsString());
        assertEquals("", json(requests[0]).get("content").getAsString());
    }

    @Test
    public void exposesAllThirtyWireActions() {
        MemoryRequest[] requests = new MemoryRequest[] {
                new CreateMemoryStoreRequest(), new GetMemoryStoreRequest(), new ListMemoryStoresRequest(),
                new UpdateMemoryStoreRequest(), new DeleteMemoryStoreRequest(), new AddMemoriesRequest(),
                new SearchMemoriesRequest(), new ListMemoriesRequest(), new GetMemoryRequest(),
                new UpdateMemoryRequest(), new DeleteMemoryRequest(), new ListMemoryStoreMessagesRequest(),
                new ListMemoryStoreRequestsRequest(), new GetMemoryTaskRequest(), new ListMemoryTasksRequest(),
                new ListMemoryStoreScopesRequest(), new CreateMemoryDreamTaskRequest(), new GetMemoryDreamTaskRequest(),
                new ListMemoryDreamTasksRequest(), new CancelMemoryDreamTaskRequest(), new ListMemoryDreamActionsRequest(),
                new ApplyMemoryDreamActionsRequest(), new AddItemRequest(), new ListItemsRequest(),
                new GetItemRequest(), new UpdateItemRequest(), new DeleteItemRequest(),
                new ListItemVersionsRequest(), new GetItemVersionRequest(), new RedactItemVersionRequest()
        };
        String[] actions = {
                "CreateMemoryStore", "GetMemoryStore", "ListMemoryStores", "UpdateMemoryStore", "DeleteMemoryStore",
                "AddMemories", "SearchMemories", "ListMemories", "GetMemory", "UpdateMemory", "DeleteMemory",
                "ListMemoryStoreMessages", "ListMemoryStoreRequests", "GetMemoryTask", "ListMemoryTasks",
                "ListMemoryStoreScopes", "CreateMemoryDreamTask", "GetMemoryDreamTask", "ListMemoryDreamTasks",
                "CancelMemoryDreamTask", "ListMemoryDreamActions", "ApplyMemoryDreamActions", "AddItem", "ListItems",
                "GetItem", "UpdateItem", "DeleteItem", "ListItemVersions", "GetItemVersion", "RedactItemVersion"
        };
        assertEquals(actions.length, requests.length);
        for (int i = 0; i < actions.length; i++) {
            assertEquals(actions[i], requests[i].getOperationName());
        }
        assertEquals(30, Arrays.asList(actions).size());
    }
}
