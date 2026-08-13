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
