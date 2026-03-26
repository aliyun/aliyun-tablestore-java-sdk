package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.model.knowledgebase.DocumentStatus;
import com.alicloud.openservices.tablestore.model.knowledgebase.RerankingType;
import com.alicloud.openservices.tablestore.model.knowledgebase.SearchType;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GsonUtilsTest {

    // ==================== Basic API Tests ====================

    @Test
    public void testGetGsonReturnsSameInstance() {
        Gson first = GsonUtils.getGson();
        Gson second = GsonUtils.getGson();
        assertSame("getGson() should return the same shared instance", first, second);
    }

    @Test
    public void testToJsonWithNull() {
        assertEquals("null", GsonUtils.toJson(null));
    }

    @Test
    public void testToJsonWithString() {
        assertEquals("\"hello\"", GsonUtils.toJson("hello"));
    }

    @Test
    public void testFromJsonWithSimpleObject() {
        String json = "{\"name\":\"test\",\"value\":42}";
        SimpleObject result = GsonUtils.fromJson(json, SimpleObject.class);
        assertEquals("test", result.name);
        assertEquals(42, result.value);
    }

    // ==================== SearchType Serialization Tests ====================

    @Test
    public void testSearchTypeSerialize_DenseVector() {
        assertEquals("\"DENSE_VECTOR\"", GsonUtils.toJson(SearchType.DENSE_VECTOR));
    }

    @Test
    public void testSearchTypeSerialize_FullText() {
        assertEquals("\"FULL_TEXT\"", GsonUtils.toJson(SearchType.FULL_TEXT));
    }

    @Test
    public void testSearchTypeSerialize_Null() {
        SearchType nullType = null;
        assertEquals("null", GsonUtils.toJson(nullType));
    }

    // ==================== SearchType Deserialization Tests ====================

    @Test
    public void testSearchTypeDeserialize_UpperCase() {
        assertEquals(SearchType.DENSE_VECTOR, GsonUtils.fromJson("\"DENSE_VECTOR\"", SearchType.class));
        assertEquals(SearchType.FULL_TEXT, GsonUtils.fromJson("\"FULL_TEXT\"", SearchType.class));
    }

    @Test
    public void testSearchTypeDeserialize_LowerCase() {
        assertEquals(SearchType.DENSE_VECTOR, GsonUtils.fromJson("\"dense_vector\"", SearchType.class));
        assertEquals(SearchType.FULL_TEXT, GsonUtils.fromJson("\"full_text\"", SearchType.class));
    }

    @Test
    public void testSearchTypeDeserialize_MixedCase() {
        assertEquals(SearchType.DENSE_VECTOR, GsonUtils.fromJson("\"Dense_Vector\"", SearchType.class));
        assertEquals(SearchType.FULL_TEXT, GsonUtils.fromJson("\"Full_Text\"", SearchType.class));
    }

    @Test
    public void testSearchTypeDeserialize_Null() {
        assertNull(GsonUtils.fromJson("null", SearchType.class));
    }

    @Test(expected = JsonSyntaxException.class)
    public void testSearchTypeDeserialize_UnknownValue() {
        GsonUtils.fromJson("\"UNKNOWN_TYPE\"", SearchType.class);
    }

    // ==================== SearchType Roundtrip Tests ====================

    @Test
    public void testSearchTypeRoundtrip() {
        for (SearchType type : SearchType.values()) {
            String json = GsonUtils.toJson(type);
            SearchType deserialized = GsonUtils.fromJson(json, SearchType.class);
            assertEquals(type, deserialized);
        }
    }

    // ==================== RerankingType Serialization Tests ====================

    @Test
    public void testRerankingTypeSerialize_AllValues() {
        assertEquals("\"RRF\"", GsonUtils.toJson(RerankingType.RRF));
        assertEquals("\"MODEL\"", GsonUtils.toJson(RerankingType.MODEL));
        assertEquals("\"WEIGHT\"", GsonUtils.toJson(RerankingType.WEIGHT));
    }

    @Test
    public void testRerankingTypeSerialize_Null() {
        RerankingType nullType = null;
        assertEquals("null", GsonUtils.toJson(nullType));
    }

    // ==================== RerankingType Deserialization Tests ====================

    @Test
    public void testRerankingTypeDeserialize_UpperCase() {
        assertEquals(RerankingType.RRF, GsonUtils.fromJson("\"RRF\"", RerankingType.class));
        assertEquals(RerankingType.MODEL, GsonUtils.fromJson("\"MODEL\"", RerankingType.class));
        assertEquals(RerankingType.WEIGHT, GsonUtils.fromJson("\"WEIGHT\"", RerankingType.class));
    }

    @Test
    public void testRerankingTypeDeserialize_LowerCase() {
        assertEquals(RerankingType.RRF, GsonUtils.fromJson("\"rrf\"", RerankingType.class));
        assertEquals(RerankingType.MODEL, GsonUtils.fromJson("\"model\"", RerankingType.class));
        assertEquals(RerankingType.WEIGHT, GsonUtils.fromJson("\"weight\"", RerankingType.class));
    }

    @Test
    public void testRerankingTypeDeserialize_MixedCase() {
        assertEquals(RerankingType.RRF, GsonUtils.fromJson("\"Rrf\"", RerankingType.class));
        assertEquals(RerankingType.MODEL, GsonUtils.fromJson("\"Model\"", RerankingType.class));
        assertEquals(RerankingType.WEIGHT, GsonUtils.fromJson("\"Weight\"", RerankingType.class));
    }

    @Test
    public void testRerankingTypeDeserialize_Null() {
        assertNull(GsonUtils.fromJson("null", RerankingType.class));
    }

    @Test(expected = JsonSyntaxException.class)
    public void testRerankingTypeDeserialize_UnknownValue() {
        GsonUtils.fromJson("\"INVALID\"", RerankingType.class);
    }

    // ==================== RerankingType Roundtrip Tests ====================

    @Test
    public void testRerankingTypeRoundtrip() {
        for (RerankingType type : RerankingType.values()) {
            String json = GsonUtils.toJson(type);
            RerankingType deserialized = GsonUtils.fromJson(json, RerankingType.class);
            assertEquals(type, deserialized);
        }
    }

    // ==================== DocumentStatus Serialization Tests ====================

    @Test
    public void testDocumentStatusSerialize_AllValues() {
        assertEquals("\"PENDING\"", GsonUtils.toJson(DocumentStatus.PENDING));
        assertEquals("\"INDEXING\"", GsonUtils.toJson(DocumentStatus.INDEXING));
        assertEquals("\"COMPLETED\"", GsonUtils.toJson(DocumentStatus.COMPLETED));
        assertEquals("\"DELETING\"", GsonUtils.toJson(DocumentStatus.DELETING));
        assertEquals("\"FAILED\"", GsonUtils.toJson(DocumentStatus.FAILED));
    }

    @Test
    public void testDocumentStatusSerialize_Null() {
        DocumentStatus nullStatus = null;
        assertEquals("null", GsonUtils.toJson(nullStatus));
    }

    // ==================== DocumentStatus Deserialization Tests ====================

    @Test
    public void testDocumentStatusDeserialize_UpperCase() {
        assertEquals(DocumentStatus.PENDING, GsonUtils.fromJson("\"PENDING\"", DocumentStatus.class));
        assertEquals(DocumentStatus.INDEXING, GsonUtils.fromJson("\"INDEXING\"", DocumentStatus.class));
        assertEquals(DocumentStatus.COMPLETED, GsonUtils.fromJson("\"COMPLETED\"", DocumentStatus.class));
        assertEquals(DocumentStatus.DELETING, GsonUtils.fromJson("\"DELETING\"", DocumentStatus.class));
        assertEquals(DocumentStatus.FAILED, GsonUtils.fromJson("\"FAILED\"", DocumentStatus.class));
    }

    @Test
    public void testDocumentStatusDeserialize_LowerCase() {
        assertEquals(DocumentStatus.PENDING, GsonUtils.fromJson("\"pending\"", DocumentStatus.class));
        assertEquals(DocumentStatus.INDEXING, GsonUtils.fromJson("\"indexing\"", DocumentStatus.class));
        assertEquals(DocumentStatus.COMPLETED, GsonUtils.fromJson("\"completed\"", DocumentStatus.class));
        assertEquals(DocumentStatus.DELETING, GsonUtils.fromJson("\"deleting\"", DocumentStatus.class));
        assertEquals(DocumentStatus.FAILED, GsonUtils.fromJson("\"failed\"", DocumentStatus.class));
    }

    @Test
    public void testDocumentStatusDeserialize_MixedCase() {
        assertEquals(DocumentStatus.PENDING, GsonUtils.fromJson("\"Pending\"", DocumentStatus.class));
        assertEquals(DocumentStatus.COMPLETED, GsonUtils.fromJson("\"Completed\"", DocumentStatus.class));
    }

    @Test
    public void testDocumentStatusDeserialize_Null() {
        assertNull(GsonUtils.fromJson("null", DocumentStatus.class));
    }

    @Test(expected = JsonSyntaxException.class)
    public void testDocumentStatusDeserialize_UnknownValue() {
        GsonUtils.fromJson("\"UNKNOWN\"", DocumentStatus.class);
    }

    // ==================== DocumentStatus Roundtrip Tests ====================

    @Test
    public void testDocumentStatusRoundtrip() {
        for (DocumentStatus status : DocumentStatus.values()) {
            String json = GsonUtils.toJson(status);
            DocumentStatus deserialized = GsonUtils.fromJson(json, DocumentStatus.class);
            assertEquals(status, deserialized);
        }
    }

    // ==================== Number Preserving Tests ====================

    @Test
    public void testNumberPreserving_IntegerAsLong() {
        String json = "{\"count\":42}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object count = result.get("count");
        assertTrue("Integer value should be deserialized as Long, but was " + count.getClass().getName(), count instanceof Long);
        assertEquals(42L, count);
    }

    @Test
    public void testNumberPreserving_FloatAsDouble() {
        String json = "{\"score\":3.14}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object score = result.get("score");
        assertTrue("Float value should be deserialized as Double, but was " + score.getClass().getName(), score instanceof Double);
        assertEquals(3.14, (Double) score, 0.001);
    }

    @Test
    public void testNumberPreserving_Zero() {
        String json = "{\"value\":0}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object value = result.get("value");
        assertTrue("Zero should be deserialized as Long", value instanceof Long);
        assertEquals(0L, value);
    }

    @Test
    public void testNumberPreserving_NegativeInteger() {
        String json = "{\"value\":-100}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object value = result.get("value");
        assertTrue("Negative integer should be deserialized as Long", value instanceof Long);
        assertEquals(-100L, value);
    }

    @Test
    public void testNumberPreserving_NegativeFloat() {
        String json = "{\"value\":-1.5}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object value = result.get("value");
        assertTrue("Negative float should be deserialized as Double", value instanceof Double);
        assertEquals(-1.5, (Double) value, 0.001);
    }

    @Test
    public void testNumberPreserving_LargeInteger() {
        String json = "{\"value\":9223372036854775807}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object value = result.get("value");
        assertTrue("Large integer should be deserialized as Long", value instanceof Long);
        assertEquals(Long.MAX_VALUE, value);
    }

    @Test
    public void testNumberPreserving_FloatWithZeroDecimal() {
        // With LONG_OR_DOUBLE strategy, 1.0 in JSON has a decimal point so it stays as Double
        String json = "{\"value\":1.0}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        Object value = result.get("value");
        assertTrue("1.0 should be deserialized as Double since JSON contains decimal point", value instanceof Double);
        assertEquals(1.0, (Double) value, 0.001);
    }

    // ==================== Number Preserving with Mixed Types ====================

    @Test
    public void testNumberPreserving_MixedTypesInMap() {
        String json = "{\"name\":\"test\",\"count\":10,\"score\":9.5,\"active\":true,\"data\":null}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);

        assertTrue("String value", result.get("name") instanceof String);
        assertEquals("test", result.get("name"));

        assertTrue("Integer value should be Long", result.get("count") instanceof Long);
        assertEquals(10L, result.get("count"));

        assertTrue("Float value should be Double", result.get("score") instanceof Double);
        assertEquals(9.5, (Double) result.get("score"), 0.001);

        assertTrue("Boolean value", result.get("active") instanceof Boolean);
        assertEquals(true, result.get("active"));

        assertNull("Null value", result.get("data"));
    }

    @Test
    public void testNumberPreserving_NestedObject() {
        String json = "{\"outer\":{\"inner_count\":5,\"inner_score\":2.5}}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);

        assertTrue("Nested object should be a Map", result.get("outer") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> nested = (Map<String, Object>) result.get("outer");
        assertTrue("Nested integer should be Long", nested.get("inner_count") instanceof Long);
        assertEquals(5L, nested.get("inner_count"));
        assertTrue("Nested float should be Double", nested.get("inner_score") instanceof Double);
    }

    @Test
    public void testNumberPreserving_Array() {
        String json = "{\"values\":[1,2.5,3,4.0]}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);

        assertTrue("Array should be a List", result.get("values") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) result.get("values");
        assertEquals(4, values.size());
        assertTrue("1 should be Long", values.get(0) instanceof Long);
        assertEquals(1L, values.get(0));
        assertTrue("2.5 should be Double", values.get(1) instanceof Double);
        assertTrue("3 should be Long", values.get(2) instanceof Long);
        assertEquals(3L, values.get(2));
        // 4.0 has a decimal point in JSON, so LONG_OR_DOUBLE keeps it as Double
        assertTrue("4.0 should be Double", values.get(3) instanceof Double);
        assertEquals(4.0, (Double) values.get(3), 0.001);
    }

    // ==================== Metadata Simulation Tests ====================

    @Test
    public void testMetadataRoundtrip_LongPreserved() {
        // Simulate the real use case: metadata with long values
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("year", 2024L);
        metadata.put("priority", 1L);
        metadata.put("score", 9.5);
        metadata.put("category", "tech");

        String json = GsonUtils.toJson(metadata);
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> deserialized = GsonUtils.getGson().fromJson(json, mapType);

        assertTrue("year should remain Long after roundtrip", deserialized.get("year") instanceof Long);
        assertEquals(2024L, deserialized.get("year"));

        assertTrue("priority should remain Long after roundtrip", deserialized.get("priority") instanceof Long);
        assertEquals(1L, deserialized.get("priority"));

        assertTrue("score should remain Double after roundtrip", deserialized.get("score") instanceof Double);
        assertEquals(9.5, (Double) deserialized.get("score"), 0.001);

        assertEquals("tech", deserialized.get("category"));
    }

    @Test
    public void testMetadataRoundtrip_WithListValues() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("tags", Arrays.asList("a", "b", "c"));
        metadata.put("scores", Arrays.asList(1, 2, 3));

        String json = GsonUtils.toJson(metadata);
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> deserialized = GsonUtils.getGson().fromJson(json, mapType);

        assertTrue("tags should be a List", deserialized.get("tags") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) deserialized.get("tags");
        assertEquals(3, tags.size());
        assertEquals("a", tags.get(0));

        assertTrue("scores should be a List", deserialized.get("scores") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> scores = (List<Object>) deserialized.get("scores");
        assertEquals(3, scores.size());
        assertTrue("score elements should be Long", scores.get(0) instanceof Long);
        assertEquals(1L, scores.get(0));
    }

    // ==================== Enum in Complex Object Tests ====================

    @Test
    public void testEnumInComplexObject_Serialization() {
        EnumContainer container = new EnumContainer();
        container.searchType = SearchType.DENSE_VECTOR;
        container.rerankingType = RerankingType.MODEL;
        container.status = DocumentStatus.COMPLETED;

        String json = GsonUtils.toJson(container);
        assertTrue(json.contains("\"DENSE_VECTOR\""));
        assertTrue(json.contains("\"MODEL\""));
        assertTrue(json.contains("\"COMPLETED\""));
    }

    @Test
    public void testEnumInComplexObject_Deserialization_MixedCase() {
        String json = "{\"searchType\":\"dense_vector\",\"rerankingType\":\"model\",\"status\":\"completed\"}";
        EnumContainer result = GsonUtils.fromJson(json, EnumContainer.class);
        assertEquals(SearchType.DENSE_VECTOR, result.searchType);
        assertEquals(RerankingType.MODEL, result.rerankingType);
        assertEquals(DocumentStatus.COMPLETED, result.status);
    }

    @Test
    public void testEnumInComplexObject_NullFields() {
        String json = "{\"searchType\":null,\"rerankingType\":null,\"status\":null}";
        EnumContainer result = GsonUtils.fromJson(json, EnumContainer.class);
        assertNull(result.searchType);
        assertNull(result.rerankingType);
        assertNull(result.status);
    }

    @Test
    public void testEnumInComplexObject_Roundtrip() {
        EnumContainer original = new EnumContainer();
        original.searchType = SearchType.FULL_TEXT;
        original.rerankingType = RerankingType.WEIGHT;
        original.status = DocumentStatus.INDEXING;

        String json = GsonUtils.toJson(original);
        EnumContainer deserialized = GsonUtils.fromJson(json, EnumContainer.class);

        assertEquals(original.searchType, deserialized.searchType);
        assertEquals(original.rerankingType, deserialized.rerankingType);
        assertEquals(original.status, deserialized.status);
    }

    // ==================== Edge Cases ====================

    @Test
    public void testEmptyJsonObject() {
        String json = "{}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testEmptyJsonArray() {
        String json = "{\"items\":[]}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        assertTrue(result.get("items") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) result.get("items");
        assertTrue(items.isEmpty());
    }

    @Test
    public void testDeeplyNestedStructure() {
        String json = "{\"l1\":{\"l2\":{\"l3\":{\"value\":42}}}}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);

        @SuppressWarnings("unchecked")
        Map<String, Object> l1 = (Map<String, Object>) result.get("l1");
        @SuppressWarnings("unchecked")
        Map<String, Object> l2 = (Map<String, Object>) l1.get("l2");
        @SuppressWarnings("unchecked")
        Map<String, Object> l3 = (Map<String, Object>) l2.get("l3");

        assertTrue("Deeply nested integer should be Long", l3.get("value") instanceof Long);
        assertEquals(42L, l3.get("value"));
    }

    @Test
    public void testBooleanValues() {
        String json = "{\"enabled\":true,\"disabled\":false}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        assertEquals(true, result.get("enabled"));
        assertEquals(false, result.get("disabled"));
    }

    @Test
    public void testStringWithSpecialCharacters() {
        String json = "{\"text\":\"hello\\nworld\\t!\"}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        assertEquals("hello\nworld\t!", result.get("text"));
    }

    @Test
    public void testEmptyString() {
        String json = "{\"text\":\"\"}";
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> result = GsonUtils.getGson().fromJson(json, mapType);
        assertEquals("", result.get("text"));
    }

    // ==================== SearchType.fromValue Tests ====================

    @Test
    public void testSearchTypeFromValue_CaseInsensitive() {
        assertEquals(SearchType.DENSE_VECTOR, SearchType.fromValue("DENSE_VECTOR"));
        assertEquals(SearchType.DENSE_VECTOR, SearchType.fromValue("dense_vector"));
        assertEquals(SearchType.DENSE_VECTOR, SearchType.fromValue("Dense_Vector"));
        assertEquals(SearchType.FULL_TEXT, SearchType.fromValue("full_text"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSearchTypeFromValue_Null() {
        SearchType.fromValue(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSearchTypeFromValue_Invalid() {
        SearchType.fromValue("INVALID");
    }

    // ==================== RerankingType.fromValue Tests ====================

    @Test
    public void testRerankingTypeFromValue_CaseInsensitive() {
        assertEquals(RerankingType.RRF, RerankingType.fromValue("RRF"));
        assertEquals(RerankingType.RRF, RerankingType.fromValue("rrf"));
        assertEquals(RerankingType.MODEL, RerankingType.fromValue("model"));
        assertEquals(RerankingType.WEIGHT, RerankingType.fromValue("Weight"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRerankingTypeFromValue_Null() {
        RerankingType.fromValue(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRerankingTypeFromValue_Invalid() {
        RerankingType.fromValue("INVALID");
    }

    // ==================== Helper Classes ====================

    private static class SimpleObject {
        String name;
        int value;
    }

    private static class EnumContainer {
        SearchType searchType;
        RerankingType rerankingType;
        DocumentStatus status;
    }
}
