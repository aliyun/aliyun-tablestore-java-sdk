package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class UpdateKnowledgeBaseRequestTest {

    // ==================== description three-state serialization ====================

    @Test
    public void testToJson_DescriptionNotSet_FieldAbsent() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertFalse("description should be absent when not set", jsonObject.has("description"));
    }

    @Test
    public void testToJson_DescriptionSetToNonNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription("My KB description");

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertEquals("My KB description", jsonObject.get("description").getAsString());
    }

    @Test
    public void testToJson_DescriptionExplicitlyNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("description should be present when explicitly set to null",
                jsonObject.has("description"));
        assertTrue("description should be JsonNull",
                jsonObject.get("description").isJsonNull());
    }

    // ==================== tags three-state serialization ====================

    @Test
    public void testToJson_TagsNotSet_FieldAbsent() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertFalse("tags should be absent when not set", jsonObject.has("tags"));
    }

    @Test
    public void testToJson_TagsSetToNonNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setTags(Arrays.asList("tag1", "tag2"));

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertEquals(2, jsonObject.getAsJsonArray("tags").size());
        assertEquals("tag1", jsonObject.getAsJsonArray("tags").get(0).getAsString());
        assertEquals("tag2", jsonObject.getAsJsonArray("tags").get(1).getAsString());
    }

    @Test
    public void testToJson_TagsExplicitlyNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setTags(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("tags should be present when explicitly set to null",
                jsonObject.has("tags"));
        assertTrue("tags should be JsonNull",
                jsonObject.get("tags").isJsonNull());
    }

    // ==================== both fields ====================

    @Test
    public void testToJson_BothDescriptionAndTagsExplicitlyNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription(null);
        request.setTags(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("description should be null", jsonObject.get("description").isJsonNull());
        assertTrue("tags should be null", jsonObject.get("tags").isJsonNull());
        assertEquals("kb1", jsonObject.get("knowledgeBaseName").getAsString());
    }

    @Test
    public void testToJson_DescriptionNullTagsNonNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription(null);
        request.setTags(Arrays.asList("tag1"));

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("description should be null", jsonObject.get("description").isJsonNull());
        assertEquals(1, jsonObject.getAsJsonArray("tags").size());
    }

    // ==================== no explicit nulls uses default Gson ====================

    @Test
    public void testToJson_NoExplicitNulls_UsesDefaultGson() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription("desc");
        request.setTags(Arrays.asList("a", "b"));

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertEquals("kb1", jsonObject.get("knowledgeBaseName").getAsString());
        assertEquals("desc", jsonObject.get("description").getAsString());
        assertEquals(2, jsonObject.getAsJsonArray("tags").size());
    }

    // ==================== custom jsonStr ====================

    @Test
    public void testToJson_CustomJsonStr_TakesPrecedence() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription(null);

        String customJson = "{\"custom\":true}";
        request.setJsonStr(customJson);

        assertEquals("Custom jsonStr should take precedence", customJson, request.toJson());
    }

    // ==================== set then override ====================

    @Test
    public void testToJson_DescriptionSetThenOverriddenToNull() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest("kb1");
        request.setDescription("original");
        request.setDescription(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("description should be null after override", jsonObject.get("description").isJsonNull());
    }

    // ==================== operation name ====================

    @Test
    public void testGetOperationName() {
        UpdateKnowledgeBaseRequest request = new UpdateKnowledgeBaseRequest();
        assertEquals("UpdateKnowledgeBase", request.getOperationName());
    }
}
