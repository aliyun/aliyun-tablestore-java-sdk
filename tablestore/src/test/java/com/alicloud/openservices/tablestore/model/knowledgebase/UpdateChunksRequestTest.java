package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class UpdateChunksRequestTest {

    // ==================== title/content three-state serialization ====================

    @Test
    public void testToJson_TitleAndContentNotSet_FieldsAbsent() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertFalse("title should be absent when not set", chunkObj.has("title"));
        assertFalse("content should be absent when not set", chunkObj.has("content"));
    }

    @Test
    public void testToJson_TitleAndContentSetToNonNull() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setTitle("My Title");
        chunk.setContent("My Content");

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertEquals("My Title", chunkObj.get("title").getAsString());
        assertEquals("My Content", chunkObj.get("content").getAsString());
    }

    @Test
    public void testToJson_TitleExplicitlyNull_ContentNotSet() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setTitle(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertTrue("title should be present when explicitly set to null", chunkObj.has("title"));
        assertTrue("title should be JsonNull", chunkObj.get("title").isJsonNull());
        assertFalse("content should be absent when not set", chunkObj.has("content"));
    }

    @Test
    public void testToJson_ContentExplicitlyNull_TitleNotSet() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setContent(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertFalse("title should be absent when not set", chunkObj.has("title"));
        assertTrue("content should be present when explicitly set to null", chunkObj.has("content"));
        assertTrue("content should be JsonNull", chunkObj.get("content").isJsonNull());
    }

    @Test
    public void testToJson_BothTitleAndContentExplicitlyNull() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setTitle(null);
        chunk.setContent(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertTrue("title should be present as null", chunkObj.has("title"));
        assertTrue("title should be JsonNull", chunkObj.get("title").isJsonNull());
        assertTrue("content should be present as null", chunkObj.has("content"));
        assertTrue("content should be JsonNull", chunkObj.get("content").isJsonNull());
    }

    // ==================== multiple chunks ====================

    @Test
    public void testToJson_MultipleChunks_OnlyOneHasExplicitNull() {
        UpdateChunksRequest.UpdateChunkItem chunk1 = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk1.setDocId("doc1");
        chunk1.setTitle("Keep this title");
        chunk1.setContent("Keep this content");

        UpdateChunksRequest.UpdateChunkItem chunk2 = new UpdateChunksRequest.UpdateChunkItem(2);
        chunk2.setDocId("doc1");
        chunk2.setTitle(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Arrays.asList(chunk1, chunk2));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonArray chunksArray = root.getAsJsonArray("chunks");
        assertEquals(2, chunksArray.size());

        JsonObject chunkObj1 = chunksArray.get(0).getAsJsonObject();
        assertEquals("Keep this title", chunkObj1.get("title").getAsString());
        assertEquals("Keep this content", chunkObj1.get("content").getAsString());

        JsonObject chunkObj2 = chunksArray.get(1).getAsJsonObject();
        assertTrue("chunk2 title should be null", chunkObj2.get("title").isJsonNull());
        assertFalse("chunk2 content should be absent", chunkObj2.has("content"));
    }

    // ==================== no explicit nulls uses default Gson ====================

    @Test
    public void testToJson_NoExplicitNulls_UsesDefaultGson() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setTitle("Title");
        chunk.setContent("Content");
        chunk.setStatus("ACTIVE");

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setSubspace("sub1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        assertEquals("kb1", root.get("knowledgeBaseName").getAsString());
        assertEquals("sub1", root.get("subspace").getAsString());

        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();
        assertEquals("doc1", chunkObj.get("docId").getAsString());
        assertEquals(1, chunkObj.get("chunkId").getAsInt());
        assertEquals("Title", chunkObj.get("title").getAsString());
        assertEquals("Content", chunkObj.get("content").getAsString());
        assertEquals("ACTIVE", chunkObj.get("status").getAsString());
    }

    // ==================== custom jsonStr ====================

    @Test
    public void testToJson_CustomJsonStr_TakesPrecedence() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setTitle(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String customJson = "{\"custom\":true}";
        request.setJsonStr(customJson);

        assertEquals("Custom jsonStr should take precedence", customJson, request.toJson());
    }

    // ==================== set then override ====================

    @Test
    public void testToJson_TitleSetThenOverriddenToNull() {
        UpdateChunksRequest.UpdateChunkItem chunk = new UpdateChunksRequest.UpdateChunkItem(1);
        chunk.setDocId("doc1");
        chunk.setTitle("Original Title");
        chunk.setTitle(null);

        UpdateChunksRequest request = new UpdateChunksRequest("kb1");
        request.setChunks(Collections.singletonList(chunk));

        String json = request.toJson();
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();
        JsonObject chunkObj = root.getAsJsonArray("chunks").get(0).getAsJsonObject();

        assertTrue("title should be present as null after override", chunkObj.has("title"));
        assertTrue("title should be JsonNull", chunkObj.get("title").isJsonNull());
    }

    // ==================== operation name ====================

    @Test
    public void testGetOperationName() {
        UpdateChunksRequest request = new UpdateChunksRequest();
        assertEquals("UpdateChunks", request.getOperationName());
    }
}
