package com.alicloud.openservices.tablestore.model.memory;

import com.alicloud.openservices.tablestore.core.http.ResponseMessage;
import com.alicloud.openservices.tablestore.core.protocol.JsonResultParser;
import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.Response;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.aliyun.ots.thirdparty.org.apache.http.ProtocolVersion;
import com.aliyun.ots.thirdparty.org.apache.http.entity.StringEntity;
import com.aliyun.ots.thirdparty.org.apache.http.message.BasicHttpResponse;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemoryResponseParsingTest {

    private void assertNoMethod(Class<?> type, String methodName) {
        try {
            type.getMethod(methodName);
            fail("Legacy response method still exists: " + type.getSimpleName() + "." + methodName + "()");
        } catch (NoSuchMethodException expected) {
            // Expected: incompatible legacy response methods must be removed.
        }
    }

    private <T extends com.alicloud.openservices.tablestore.model.Response> T parse(
            String json, Class<T> type) throws Exception {
        BasicHttpResponse response = new BasicHttpResponse(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
        response.addHeader("x-ots-requestid", "request-1");
        response.setEntity(new StringEntity(json, "UTF-8"));
        return type.cast(new JsonResultParser<T>(type, "trace-1").getObject(new ResponseMessage(response)));
    }

    @Test
    public void parsesMemorySearchAndSnakeCaseFields() throws Exception {
        SearchMemoriesResponse response = parse(
                "{\"memoryStoreName\":\"store\",\"results\":[{\"score\":0.9,\"similarity\":0.8," +
                        "\"source\":\"vector\",\"unit\":{\"id\":\"m1\",\"conversation_key\":\"c1\"," +
                        "\"scope\":{\"appId\":\"app\"},\"created_at\":\"now\"}}]}",
                SearchMemoriesResponse.class);
        assertEquals("request-1", response.getRequestId());
        assertEquals("trace-1", response.getTraceId());
        assertEquals("store", response.getMemoryStoreName());
        assertEquals("c1", response.getResults().get(0).getUnit().getConversationKey());
        assertEquals("now", response.getResults().get(0).getUnit().getCreatedAt());
        assertEquals(0.8D, response.getResults().get(0).getSimilarity(), 0D);
    }

    @Test
    public void parsesDreamAndItemResponses() throws Exception {
        GetMemoryDreamTaskResponse dream = parse(
                "{\"memoryStoreName\":\"store\",\"dreamId\":\"d1\",\"status\":\"completed\"," +
                        "\"taskType\":\"profile\",\"applyMode\":\"proposal\",\"scopeOutputMode\":\"preserve_scope\"," +
                        "\"input\":{\"scopes\":[],\"minTimestamp\":1,\"maxTimestamp\":2,\"sessionCount\":1," +
                        "\"messageCount\":2,\"memoryCount\":3},\"actions\":{\"total\":1,\"proposed\":1," +
                        "\"applied\":0,\"skipped\":0,\"failed\":0},\"createdAt\":\"c\",\"updatedAt\":\"u\"}",
                GetMemoryDreamTaskResponse.class);
        assertEquals("profile", dream.getTaskType());
        assertEquals(Integer.valueOf(3), dream.getInput().getMemoryCount());
        assertEquals(Integer.valueOf(1), dream.getActions().getTotal());

        ListItemsResponse items = parse(
                "{\"type\":\"memoryfile\",\"readOnly\":true,\"items\":[{\"itemId\":\"i1\"," +
                        "\"path\":\"/a.md\",\"contentSha256\":\"sha\",\"contentSizeBytes\":5," +
                        "\"latestSeq\":2,\"createdAt\":\"c\",\"updatedAt\":\"u\"}]}",
                ListItemsResponse.class);
        assertEquals(Boolean.TRUE, items.getReadOnly());
        assertEquals("i1", items.getItems().get(0).getItemId());
        assertEquals(Long.valueOf(2L), items.getItems().get(0).getLatestSeq());
    }

    @Test
    public void parsesRevisedItemResponseContract() throws Exception {
        GetItemResponse item = parse(
                "{\"type\":\"memoryfile\",\"itemId\":\"item-1\",\"scope\":{" +
                        "\"appId\":\"app\",\"tenantId\":\"tenant\",\"agentId\":\"agent\"," +
                        "\"runId\":\"run\"},\"path\":\"/a.md\",\"content\":\"\"," +
                        "\"contentSha256\":\"sha\",\"contentSizeBytes\":0,\"latestSeq\":1," +
                        "\"latestVersionId\":\"fmver-1\",\"createdAt\":\"c\",\"updatedAt\":\"u\"}",
                GetItemResponse.class);
        assertEquals("app", item.getScope().getAppId());
        assertEquals("fmver-1", item.getLatestVersionId());

        ListItemsResponse items = parse(
                "{\"type\":\"memoryfile\",\"items\":[{\"type\":\"memoryfile\"," +
                        "\"itemId\":\"item-1\",\"scope\":{\"appId\":\"app\"," +
                        "\"tenantId\":\"tenant\",\"agentId\":\"agent\",\"runId\":\"run\"}," +
                        "\"path\":\"/a.md\",\"latestVersionId\":\"fmver-1\"},{" +
                        "\"type\":\"memoryfile_prefix\",\"scope\":{\"appId\":\"app\"," +
                        "\"tenantId\":\"tenant\",\"agentId\":\"agent\",\"runId\":\"run\"}," +
                        "\"path\":\"/archive/\"}]}",
                ListItemsResponse.class);
        assertEquals("tenant", items.getItems().get(0).getScope().getTenantId());
        assertEquals("fmver-1", items.getItems().get(0).getLatestVersionId());
        assertEquals("memoryfile_prefix", items.getItems().get(1).getType());

        GetItemVersionResponse version = parse(
                "{\"type\":\"memoryfile\",\"versionId\":\"fmver-1\",\"itemId\":\"item-1\"," +
                        "\"scope\":{\"appId\":\"app\",\"tenantId\":\"tenant\"," +
                        "\"agentId\":\"agent\",\"runId\":\"run\"},\"operation\":\"modified\"," +
                        "\"path\":\"/a.md\",\"contentSha256\":\"sha\",\"contentSizeBytes\":1," +
                        "\"createdBy\":{\"actorType\":\"session\",\"actorId\":\"s1\"}," +
                        "\"createdAt\":\"c\",\"redacted\":true,\"redactedAt\":\"r\"," +
                        "\"redactedBy\":{\"actorType\":\"api_key\",\"actorId\":\"k1\"}}",
                GetItemVersionResponse.class);
        assertEquals("session", version.getCreatedBy().getActorType());
        assertEquals("k1", version.getRedactedBy().getActorId());

        ListItemVersionsResponse versions = parse(
                "{\"type\":\"memoryfile\",\"versions\":[{\"versionId\":\"fmver-1\"," +
                        "\"itemId\":\"item-1\",\"scope\":{\"appId\":\"app\"," +
                        "\"tenantId\":\"tenant\",\"agentId\":\"agent\",\"runId\":\"run\"}," +
                        "\"operation\":\"modified\",\"path\":\"/a.md\",\"contentSha256\":\"sha\"," +
                        "\"contentSizeBytes\":1,\"createdBy\":{\"actorType\":\"session\"," +
                        "\"actorId\":\"s1\"},\"createdAt\":\"c\",\"redacted\":true," +
                        "\"redactedAt\":\"r\",\"redactedBy\":{\"actorType\":\"api_key\"," +
                        "\"actorId\":\"k1\"}}]}",
                ListItemVersionsResponse.class);
        ItemVersion listedVersion = versions.getVersions().get(0);
        assertEquals("run", listedVersion.getScope().getRunId());
        assertEquals("session", listedVersion.getCreatedBy().getActorType());
        assertEquals("k1", listedVersion.getRedactedBy().getActorId());

        DeleteItemResponse deleted = parse(
                "{\"type\":\"memoryfile\",\"itemId\":\"item-1\",\"deleted\":true}",
                DeleteItemResponse.class);
        assertEquals("memoryfile", deleted.getType());
        assertEquals("item-1", deleted.getItemId());
        assertEquals(Boolean.TRUE, deleted.getDeleted());

        DeleteItemResponse pending = parse(
                "{\"type\":\"memoryfile\",\"taskId\":\"fmdel-1\",\"status\":\"pending\"}",
                DeleteItemResponse.class);
        assertEquals("fmdel-1", pending.getTaskId());
        assertEquals("pending", pending.getStatus());

        assertNoMethod(ItemVersion.class, "getVersionSeq");
        assertNoMethod(ItemVersion.class, "getSessionId");
        assertNoMethod(ItemVersionResponse.class, "getVersionSeq");
        assertNoMethod(ItemVersionResponse.class, "getSessionId");
    }

    @Test
    public void preservesMemoryAndTransportRequestIds() throws Exception {
        AddMemoriesResponse response = parse(
                "{\"requestId\":\"memory-request-1\",\"status\":\"accepted\"," +
                        "\"acceptedMessages\":2,\"memoryStoreName\":\"store\"}",
                AddMemoriesResponse.class);

        assertEquals("request-1", response.getRequestId());
        assertEquals("memory-request-1", response.getMemoryRequestId());
    }

    @Test
    public void keepsExistingResponseRequestIdGsonBehavior() {
        Response response = new Response("transport-request-1");
        response.setTraceId("trace-1");

        JsonObject json = new JsonParser().parse(GsonUtils.toJson(response)).getAsJsonObject();

        assertEquals("transport-request-1", json.get("requestId").getAsString());
        assertEquals("trace-1", json.get("traceId").getAsString());
    }
}
