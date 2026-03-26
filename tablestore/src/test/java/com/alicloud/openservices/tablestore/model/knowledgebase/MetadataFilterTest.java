package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class MetadataFilterTest {

    private final Gson gson = GsonUtils.getGson();

    @Test
    public void testEqualsFilter() {
        // Test string value
        MetadataFilter filter = MetadataFilter.equals("name", "test");
        String json = filter.toJson();
        System.out.println("Equals filter (string):");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("equals"));
        assertEquals("name", jsonObj.getAsJsonObject("equals").get("key").getAsString());
        assertEquals("test", jsonObj.getAsJsonObject("equals").get("value").getAsString());

        // Test number value
        MetadataFilter numberFilter = MetadataFilter.equals("year", 1989);
        String numberJson = numberFilter.toJson();
        System.out.println("\nEquals filter (number):");
        System.out.println(numberJson);
        
        JsonObject numberJsonObj = gson.fromJson(numberJson, JsonObject.class);
        assertTrue(numberJsonObj.has("equals"));
        assertEquals(1989, numberJsonObj.getAsJsonObject("equals").get("value").getAsInt());
    }

    @Test
    public void testNotEqualsFilter() {
        MetadataFilter filter = MetadataFilter.notEquals("status", "inactive");
        String json = filter.toJson();
        System.out.println("NotEquals filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("notEquals"));
        assertEquals("status", jsonObj.getAsJsonObject("notEquals").get("key").getAsString());
    }

    @Test
    public void testGreaterThanFilter() {
        MetadataFilter filter = MetadataFilter.greaterThan("age", 18);
        String json = filter.toJson();
        System.out.println("GreaterThan filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("greaterThan"));
        assertEquals(18, jsonObj.getAsJsonObject("greaterThan").get("value").getAsInt());
    }

    @Test
    public void testGreaterThanOrEqualsFilter() {
        MetadataFilter filter = MetadataFilter.greaterThanOrEquals("score", 90.5);
        String json = filter.toJson();
        System.out.println("GreaterThanOrEquals filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("greaterThanOrEquals"));
        assertEquals(90.5, jsonObj.getAsJsonObject("greaterThanOrEquals").get("value").getAsDouble(), 0.01);
    }

    @Test
    public void testLessThanFilter() {
        MetadataFilter filter = MetadataFilter.lessThan("price", 100);
        String json = filter.toJson();
        System.out.println("LessThan filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("lessThan"));
    }

    @Test
    public void testLessThanOrEqualsFilter() {
        MetadataFilter filter = MetadataFilter.lessThanOrEquals("quantity", 50);
        String json = filter.toJson();
        System.out.println("LessThanOrEquals filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("lessThanOrEquals"));
    }

    @Test
    public void testInFilter() {
        MetadataFilter filter = MetadataFilter.in("category", "electronics", "books", "clothing");
        String json = filter.toJson();
        System.out.println("In filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("in"));
        assertEquals(3, jsonObj.getAsJsonObject("in").getAsJsonArray("value").size());
    }

    @Test
    public void testNotInFilter() {
        MetadataFilter filter = MetadataFilter.notIn("status", Arrays.asList("deleted", "archived"));
        String json = filter.toJson();
        System.out.println("NotIn filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("notIn"));
    }

    @Test
    public void testStartsWithFilter() {
        MetadataFilter filter = MetadataFilter.startsWith("filename", "report_");
        String json = filter.toJson();
        System.out.println("StartsWith filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("startsWith"));
        assertEquals("report_", jsonObj.getAsJsonObject("startsWith").get("value").getAsString());
    }

    @Test
    public void testStringContainsFilter() {
        MetadataFilter filter = MetadataFilter.stringContains("description", "important");
        String json = filter.toJson();
        System.out.println("StringContains filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("stringContains"));
    }

    @Test
    public void testListContainsFilter() {
        MetadataFilter filter = MetadataFilter.listContains("tags", "urgent");
        String json = filter.toJson();
        System.out.println("ListContains filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("listContains"));
    }

    @Test
    public void testAndAllFilter() {
        // Test case from user's example
        MetadataFilter filter = MetadataFilter.andAll(
            MetadataFilter.equals("name", "ccc"),
            MetadataFilter.equals("type", 111)
        );
        
        String json = filter.toJson();
        System.out.println("AndAll filter (user example):");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("andAll"));
        assertEquals(2, jsonObj.getAsJsonArray("andAll").size());
        
        // Verify first condition
        JsonObject firstCondition = jsonObj.getAsJsonArray("andAll").get(0).getAsJsonObject();
        assertTrue(firstCondition.has("equals"));
        assertEquals("name", firstCondition.getAsJsonObject("equals").get("key").getAsString());
        assertEquals("ccc", firstCondition.getAsJsonObject("equals").get("value").getAsString());
        
        // Verify second condition
        JsonObject secondCondition = jsonObj.getAsJsonArray("andAll").get(1).getAsJsonObject();
        assertTrue(secondCondition.has("equals"));
        assertEquals("type", secondCondition.getAsJsonObject("equals").get("key").getAsString());
        assertEquals(111, secondCondition.getAsJsonObject("equals").get("value").getAsInt());
    }

    @Test
    public void testOrAllFilter() {
        MetadataFilter filter = MetadataFilter.orAll(
            MetadataFilter.equals("status", "active"),
            MetadataFilter.equals("status", "pending")
        );
        
        String json = filter.toJson();
        System.out.println("OrAll filter:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("orAll"));
        assertEquals(2, jsonObj.getAsJsonArray("orAll").size());
    }

    @Test
    public void testNestedLogicalFilters() {
        // Test nested andAll and orAll
        MetadataFilter filter = MetadataFilter.andAll(
            MetadataFilter.equals("category", "electronics"),
            MetadataFilter.orAll(
                MetadataFilter.greaterThan("price", 100),
                MetadataFilter.equals("brand", "premium")
            ),
            MetadataFilter.in("status", "available", "in_stock")
        );
        
        String json = filter.toJson();
        System.out.println("Nested logical filters:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("andAll"));
        assertEquals(3, jsonObj.getAsJsonArray("andAll").size());
        
        // Verify nested orAll
        JsonObject nestedOr = jsonObj.getAsJsonArray("andAll").get(1).getAsJsonObject();
        assertTrue(nestedOr.has("orAll"));
        assertEquals(2, nestedOr.getAsJsonArray("orAll").size());
    }

    @Test
    public void testBuilderSimple() {
        MetadataFilter filter = MetadataFilter.builder()
            .equals("name", "test")
            .greaterThan("age", 18)
            .buildAnd();
        
        String json = filter.toJson();
        System.out.println("Builder simple (AND):");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("andAll"));
        assertEquals(2, jsonObj.getAsJsonArray("andAll").size());
    }

    @Test
    public void testBuilderWithOr() {
        MetadataFilter filter = MetadataFilter.builder()
            .equals("status", "active")
            .equals("status", "pending")
            .buildOr();
        
        String json = filter.toJson();
        System.out.println("Builder simple (OR):");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("orAll"));
        assertEquals(2, jsonObj.getAsJsonArray("orAll").size());
    }

    @Test
    public void testBuilderComplex() {
        // Build a complex nested filter using builder
        MetadataFilter priceFilter = MetadataFilter.builder()
            .greaterThan("price", 50)
            .lessThan("price", 200)
            .buildAnd();
        
        MetadataFilter categoryFilter = MetadataFilter.builder()
            .equals("category", "electronics")
            .equals("category", "books")
            .buildOr();
        
        MetadataFilter finalFilter = MetadataFilter.andAll(
            priceFilter,
            categoryFilter,
            MetadataFilter.in("status", "available", "in_stock")
        );
        
        String json = finalFilter.toJson();
        System.out.println("Builder complex nested:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("andAll"));
        assertEquals(3, jsonObj.getAsJsonArray("andAll").size());
    }

    @Test
    public void testBuilderSingleCondition() {
        // When builder has only one condition, it should return that condition directly
        MetadataFilter filter = MetadataFilter.builder()
            .equals("name", "test")
            .buildAnd();
        
        String json = filter.toJson();
        System.out.println("Builder single condition:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("equals"));
        assertFalse(jsonObj.has("andAll"));
    }

    @Test
    public void testComplexRealWorldScenario() {
        // Real-world scenario: Find documents that are:
        // - (category is "report" OR "document") AND
        // - created in 2023 AND
        // - (status is "published" OR "reviewed") AND
        // - tags contain "important"
        
        MetadataFilter categoryFilter = MetadataFilter.orAll(
            MetadataFilter.equals("category", "report"),
            MetadataFilter.equals("category", "document")
        );
        
        MetadataFilter statusFilter = MetadataFilter.orAll(
            MetadataFilter.equals("status", "published"),
            MetadataFilter.equals("status", "reviewed")
        );
        
        MetadataFilter finalFilter = MetadataFilter.andAll(
            categoryFilter,
            MetadataFilter.equals("year", 2023),
            statusFilter,
            MetadataFilter.listContains("tags", "important")
        );
        
        String json = finalFilter.toJson();
        System.out.println("Complex real-world scenario:");
        System.out.println(json);
        
        JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
        assertTrue(jsonObj.has("andAll"));
        assertEquals(4, jsonObj.getAsJsonArray("andAll").size());
    }

    @Test(expected = IllegalStateException.class)
    public void testBuilderEmptyThrowsException() {
        MetadataFilter.builder().buildAnd();
    }

    @Test
    public void testAllOperatorsInOneTest() {
        System.out.println("\n=== Testing All Operators ===\n");
        
        // Basic comparison
        System.out.println("1. Equals: " + MetadataFilter.equals("key", "value").toJson());
        System.out.println("\n2. NotEquals: " + MetadataFilter.notEquals("key", "value").toJson());
        System.out.println("\n3. GreaterThan: " + MetadataFilter.greaterThan("key", 10).toJson());
        System.out.println("\n4. GreaterThanOrEquals: " + MetadataFilter.greaterThanOrEquals("key", 10).toJson());
        System.out.println("\n5. LessThan: " + MetadataFilter.lessThan("key", 10).toJson());
        System.out.println("\n6. LessThanOrEquals: " + MetadataFilter.lessThanOrEquals("key", 10).toJson());
        
        // List operations
        System.out.println("\n7. In: " + MetadataFilter.in("key", "a", "b", "c").toJson());
        System.out.println("\n8. NotIn: " + MetadataFilter.notIn("key", "x", "y").toJson());
        
        // String operations
        System.out.println("\n9. StartsWith: " + MetadataFilter.startsWith("key", "prefix").toJson());
        System.out.println("\n10. StringContains: " + MetadataFilter.stringContains("key", "substring").toJson());
        System.out.println("\n11. ListContains: " + MetadataFilter.listContains("key", "item").toJson());
        
        // Logical operations
        System.out.println("\n12. AndAll: " + MetadataFilter.andAll(
            MetadataFilter.equals("a", 1),
            MetadataFilter.equals("b", 2)
        ).toJson());
        
        System.out.println("\n13. OrAll: " + MetadataFilter.orAll(
            MetadataFilter.equals("a", 1),
            MetadataFilter.equals("b", 2)
        ).toJson());
    }
}
