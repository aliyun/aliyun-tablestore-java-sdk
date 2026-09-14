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

    private void assertJsonEquals(String expected, JsonObject actual) {
        assertEquals(new JsonParser().parse(expected), actual);
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
        GetItemRequest get = new GetItemRequest("store", SCOPE);
        get.setPath("/a.md");
        DeleteItemRequest delete = new DeleteItemRequest("store", SCOPE);
        delete.setPath("/a.md");
        ListItemVersionsRequest versions = new ListItemVersionsRequest("store", SCOPE);
        versions.setItemId("item-1");
        MemoryRequest[] requests = new MemoryRequest[] {
                new AddItemRequest("store", SCOPE, "/a.md", ""),
                new ListItemsRequest("store", SCOPE),
                get,
                new UpdateItemRequest("store", SCOPE, "item-1"),
                delete,
                versions,
                new GetItemVersionRequest("store", SCOPE, "version-1"),
                new RedactItemVersionRequest("store", SCOPE, "version-1")
        };
        for (MemoryRequest request : requests) {
            assertEquals("memoryfile", json(request).get("type").getAsString());
        }
        ((GetItemRequest) requests[2]).setType("customtype");
        assertEquals("customtype", json(requests[2]).get("type").getAsString());
        assertEquals("", json(requests[0]).get("content").getAsString());
    }

    @Test
    public void itemRequestContractMatchesRevisedService() {
        ItemPrecondition precondition = new ItemPrecondition("fmver-1");
        assertEquals("fmver-1", precondition.getExpectedVersionId());
        precondition.setExpectedVersionId("fmver-2");
        assertEquals("fmver-2", precondition.getExpectedVersionId());

        assertEquals("memoryfile_prefix", MemoryConstants.ITEM_TYPE_MEMORY_FILE_PREFIX);
        assertEquals("memoryfile", MemoryConstants.ITEM_TYPE_MEMORY_FILE);
        assertEquals("basic", MemoryConstants.ITEM_VIEW_BASIC);
        assertEquals("full", MemoryConstants.ITEM_VIEW_FULL);
        assertEquals("created", MemoryConstants.ITEM_OPERATION_CREATED);
        assertEquals("modified", MemoryConstants.ITEM_OPERATION_MODIFIED);
        assertEquals("deleted", MemoryConstants.ITEM_OPERATION_DELETED);
        assertEquals("session", MemoryConstants.ITEM_ACTOR_TYPE_SESSION);
        assertEquals("api_key", MemoryConstants.ITEM_ACTOR_TYPE_API_KEY);
        assertEquals("user", MemoryConstants.ITEM_ACTOR_TYPE_USER);
        assertEquals("pending", MemoryConstants.ITEM_DELETE_STATUS_PENDING);
        assertEquals(102400, MemoryConstants.MAX_ITEM_CONTENT_BYTES);
        assertEquals(1024, MemoryConstants.MAX_ITEM_PATH_BYTES);
        assertEquals(128, MemoryConstants.MAX_ITEM_SCOPE_FIELD_BYTES);
        assertEquals(128, MemoryConstants.MAX_ITEM_ID_BYTES);
        assertEquals(100, MemoryConstants.MAX_ITEM_LIST_LIMIT);
        assertEquals(20, MemoryConstants.MAX_ITEM_FULL_LIST_LIMIT);
        assertEquals(2000, MemoryConstants.MAX_ITEMS_PER_SCOPE);

        AddItemRequest add = new AddItemRequest("store", SCOPE, "/a.md", "");
        add.setView(MemoryConstants.ITEM_VIEW_FULL);
        add.setSessionId("s1");
        JsonObject addJson = json(add);
        assertJsonEquals("{\"path\":\"/a.md\",\"content\":\"\",\"sessionId\":\"s1\"," +
                "\"view\":\"full\",\"type\":\"memoryfile\",\"memoryStoreName\":\"store\"," +
                "\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\",\"agentId\":\"agent\"," +
                "\"runId\":\"run\"}}", addJson);

        GetItemRequest get = new GetItemRequest("store", SCOPE);
        get.setItemId("item-1");
        get.setView(MemoryConstants.ITEM_VIEW_BASIC);
        JsonObject getJson = json(get);
        assertJsonEquals("{\"itemId\":\"item-1\",\"view\":\"basic\",\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"agent\",\"runId\":\"run\"}}", getJson);

        UpdateItemRequest update = new UpdateItemRequest("store", SCOPE, "item-1");
        update.setContent("");
        update.setPath("/renamed.md");
        update.setPrecondition(new ItemPrecondition("fmver-1"));
        update.setOverwrite(false);
        update.setSessionId("s2");
        update.setView(MemoryConstants.ITEM_VIEW_FULL);
        JsonObject updateJson = json(update);
        assertJsonEquals("{\"itemId\":\"item-1\",\"content\":\"\",\"path\":\"/renamed.md\"," +
                "\"precondition\":{\"expectedVersionId\":\"fmver-1\"},\"overwrite\":false," +
                "\"sessionId\":\"s2\",\"view\":\"full\",\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"agent\",\"runId\":\"run\"}}", updateJson);

        DeleteItemRequest exactDelete = new DeleteItemRequest("store", SCOPE);
        exactDelete.setItemId("item-1");
        exactDelete.setPrecondition(new ItemPrecondition("fmver-1"));
        JsonObject exactDeleteJson = json(exactDelete);
        assertJsonEquals("{\"itemId\":\"item-1\",\"precondition\":{\"expectedVersionId\":\"fmver-1\"}," +
                "\"type\":\"memoryfile\",\"memoryStoreName\":\"store\",\"scope\":{" +
                "\"appId\":\"app\",\"tenantId\":\"tenant\",\"agentId\":\"agent\"," +
                "\"runId\":\"run\"}}", exactDeleteJson);

        Scope wildcardScope = new Scope("app", "tenant", "*", "*");
        DeleteItemRequest delete = new DeleteItemRequest("store", wildcardScope);
        delete.setPathPrefix("/profile/");
        delete.setSessionId("s3");
        JsonObject deleteJson = json(delete);
        assertJsonEquals("{\"pathPrefix\":\"/profile/\",\"sessionId\":\"s3\",\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"*\",\"runId\":\"*\"}}", deleteJson);

        ListItemsRequest listItems = new ListItemsRequest("store", wildcardScope);
        listItems.setPathPrefix("/profile/");
        listItems.setDepth(1);
        listItems.setView(MemoryConstants.ITEM_VIEW_FULL);
        listItems.setLimit(20);
        listItems.setNextToken("item-next");
        JsonObject listItemsJson = json(listItems);
        assertJsonEquals("{\"pathPrefix\":\"/profile/\",\"depth\":1,\"view\":\"full\"," +
                "\"nextToken\":\"item-next\",\"limit\":20,\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"*\",\"runId\":\"*\"}}", listItemsJson);

        ListItemVersionsRequest versions = new ListItemVersionsRequest("store", wildcardScope);
        versions.setItemId("item-1");
        versions.setSessionId("s4");
        versions.setCreatedAtGte("2026-08-01T00:00:00Z");
        versions.setCreatedAtLte("2026-08-20T00:00:00Z");
        versions.setOperation("modified");
        versions.setView(MemoryConstants.ITEM_VIEW_FULL);
        versions.setLimit(20);
        versions.setNextToken("next");
        JsonObject versionsJson = json(versions);
        assertJsonEquals("{\"itemId\":\"item-1\",\"sessionId\":\"s4\"," +
                "\"createdAtGte\":\"2026-08-01T00:00:00Z\"," +
                "\"createdAtLte\":\"2026-08-20T00:00:00Z\",\"operation\":\"modified\"," +
                "\"view\":\"full\",\"nextToken\":\"next\",\"limit\":20,\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"*\",\"runId\":\"*\"}}", versionsJson);

        GetItemVersionRequest getVersion = new GetItemVersionRequest("store", SCOPE, "fmver-1");
        getVersion.setView(MemoryConstants.ITEM_VIEW_FULL);
        assertJsonEquals("{\"versionId\":\"fmver-1\",\"view\":\"full\",\"type\":\"memoryfile\"," +
                "\"memoryStoreName\":\"store\",\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                "\"agentId\":\"agent\",\"runId\":\"run\"}}", json(getVersion));

        RedactItemVersionRequest redact = new RedactItemVersionRequest("store", SCOPE, "fmver-1");
        redact.setSessionId("privacy-job");
        assertJsonEquals("{\"versionId\":\"fmver-1\",\"sessionId\":\"privacy-job\"," +
                "\"type\":\"memoryfile\",\"memoryStoreName\":\"store\",\"scope\":{" +
                "\"appId\":\"app\",\"tenantId\":\"tenant\",\"agentId\":\"agent\"," +
                "\"runId\":\"run\"}}", json(redact));

        assertNoMethod(GetItemRequest.class, "getIncludeContent");
        assertNoMethod(GetItemRequest.class, "setIncludeContent", Boolean.class);
        assertNoMethod(UpdateItemRequest.class, "getNewPath");
        assertNoMethod(UpdateItemRequest.class, "setNewPath", String.class);
        assertNoMethod(UpdateItemRequest.class, "getExpectedSha256");
        assertNoMethod(UpdateItemRequest.class, "setExpectedSha256", String.class);
        assertNoMethod(DeleteItemRequest.class, "getExpectedSha256");
        assertNoMethod(DeleteItemRequest.class, "setExpectedSha256", String.class);
        assertNoMethod(ListItemVersionsRequest.class, "getPath");
        assertNoMethod(ListItemVersionsRequest.class, "setPath", String.class);
        assertNoMethod(GetItemVersionRequest.class, "getItemId");
        assertNoMethod(GetItemVersionRequest.class, "setItemId", String.class);
        assertNoMethod(GetItemVersionRequest.class, "getVersionSeq");
        assertNoMethod(GetItemVersionRequest.class, "setVersionSeq", Long.class);
        assertNoMethod(RedactItemVersionRequest.class, "getItemId");
        assertNoMethod(RedactItemVersionRequest.class, "setItemId", String.class);
        assertNoMethod(RedactItemVersionRequest.class, "getVersionSeq");
        assertNoMethod(RedactItemVersionRequest.class, "setVersionSeq", Long.class);

        assertNoConstructor(GetItemRequest.class, String.class, Scope.class, String.class);
        assertNoConstructor(DeleteItemRequest.class, String.class, Scope.class, String.class);
        assertNoConstructor(ListItemVersionsRequest.class, String.class, Scope.class, String.class);
        assertNoConstructor(GetItemVersionRequest.class,
                String.class, Scope.class, String.class, String.class, Long.class);
        assertNoConstructor(RedactItemVersionRequest.class,
                String.class, Scope.class, String.class, String.class, Long.class);
    }

    private static void assertNoMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            type.getMethod(name, parameterTypes);
            fail("legacy method must be removed: " + type.getSimpleName() + "." + name);
        } catch (NoSuchMethodException expected) {
            // Expected: forward compatibility with the old Item contract is intentionally unsupported.
        }
    }

    private static void assertNoConstructor(Class<?> type, Class<?>... parameterTypes) {
        try {
            type.getConstructor(parameterTypes);
            fail("legacy constructor must be removed from " + type.getSimpleName());
        } catch (NoSuchMethodException expected) {
            // Expected: forward compatibility with the old Item contract is intentionally unsupported.
        }
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
