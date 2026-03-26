package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class UpdateDocumentRequestTest {

    // ==================== metadata serialization: three states ====================

    @Test
    public void testToJson_MetadataNotSet_FieldAbsent() {
        // When setMetadata is never called, the JSON should not contain "metadata" at all
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertFalse("metadata field should be absent when not set",
                jsonObject.has("metadata"));
        assertEquals("kb1", jsonObject.get("knowledgeBaseName").getAsString());
        assertEquals("doc1", jsonObject.get("docId").getAsString());
    }

    @Test
    public void testToJson_MetadataSetToNonNull_FieldPresent() {
        // When setMetadata is called with a non-null map, JSON should contain "metadata": {...}
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("year", 2024L);
        metadata.put("category", "tech");
        request.setMetadata(metadata);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("metadata field should be present", jsonObject.has("metadata"));
        JsonObject metadataObj = jsonObject.getAsJsonObject("metadata");
        assertEquals(2024L, metadataObj.get("year").getAsLong());
        assertEquals("tech", metadataObj.get("category").getAsString());
    }

    @Test
    public void testToJson_MetadataExplicitlySetToNull_FieldPresentAsNull() {
        // When setMetadata(null) is called explicitly, JSON should contain "metadata": null
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");
        request.setMetadata(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("metadata field should be present when explicitly set to null",
                jsonObject.has("metadata"));
        assertTrue("metadata should be JsonNull",
                jsonObject.get("metadata").isJsonNull());
    }

    // ==================== other fields ====================

    @Test
    public void testToJson_OssKeyNotSet_FieldAbsent() {
        // ossKey not set should not appear in JSON
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertFalse("ossKey should be absent when not set", jsonObject.has("ossKey"));
    }

    @Test
    public void testToJson_AllFieldsSet() {
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setSubspace("sub1");
        request.setOssKey("oss://bucket/key");
        request.setDocId("doc1");

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("priority", 1L);
        request.setMetadata(metadata);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertEquals("kb1", jsonObject.get("knowledgeBaseName").getAsString());
        assertEquals("sub1", jsonObject.get("subspace").getAsString());
        assertEquals("oss://bucket/key", jsonObject.get("ossKey").getAsString());
        assertEquals("doc1", jsonObject.get("docId").getAsString());
        assertEquals(1L, jsonObject.getAsJsonObject("metadata").get("priority").getAsLong());
    }

    // ==================== custom jsonStr ====================

    @Test
    public void testToJson_CustomJsonStr_TakesPrecedence() {
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");
        request.setMetadata(null);

        String customJson = "{\"custom\":true}";
        request.setJsonStr(customJson);

        assertEquals("Custom jsonStr should take precedence", customJson, request.toJson());
    }

    // ==================== metadata set then overridden ====================

    @Test
    public void testToJson_MetadataSetThenOverriddenToNull() {
        // First set metadata to a value, then override to null
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("key", "value");
        request.setMetadata(metadata);
        request.setMetadata(null);

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("metadata should be present as null after override",
                jsonObject.has("metadata"));
        assertTrue("metadata should be JsonNull after override",
                jsonObject.get("metadata").isJsonNull());
    }

    @Test
    public void testToJson_MetadataSetToEmptyMap() {
        // Empty map is a valid metadata value, should be serialized as {}
        UpdateDocumentRequest request = new UpdateDocumentRequest("kb1");
        request.setDocId("doc1");
        request.setMetadata(new HashMap<>());

        String json = request.toJson();
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();

        assertTrue("metadata should be present", jsonObject.has("metadata"));
        assertFalse("metadata should not be null", jsonObject.get("metadata").isJsonNull());
        assertEquals(0, jsonObject.getAsJsonObject("metadata").size());
    }

    // ==================== operation name ====================

    @Test
    public void testGetOperationName() {
        UpdateDocumentRequest request = new UpdateDocumentRequest();
        assertEquals("UpdateDocument", request.getOperationName());
    }
}
