package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestPrimaryKeySchema {

    @Test
    public void testConstructor() {
        PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.BINARY);
        assertEquals(schema.getName(), "A");
        assertEquals(schema.getType(), PrimaryKeyType.BINARY);

        PrimaryKeySchema schema2 = new PrimaryKeySchema("B", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
        assertEquals(schema2.getName(), "B");
        assertEquals(schema2.getType(), PrimaryKeyType.INTEGER);
        assertEquals(schema2.getOption(), PrimaryKeyOption.AUTO_INCREMENT);

        try {
            new PrimaryKeySchema(null, PrimaryKeyType.BINARY);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            new PrimaryKeySchema("", PrimaryKeyType.BINARY);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            new PrimaryKeySchema("A", null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, null);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new PrimaryKeySchema("A", PrimaryKeyType.BINARY, PrimaryKeyOption.AUTO_INCREMENT);
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            new PrimaryKeySchema("A", PrimaryKeyType.STRING, PrimaryKeyOption.AUTO_INCREMENT);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testEqual() {
        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            assertTrue(schemaA.equals(schemaB));
        }

        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            assertTrue(!schemaA.equals(schemaB));
        }

        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("B", PrimaryKeyType.INTEGER);
            assertTrue(!schemaA.equals(schemaB));
        }

        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("A", PrimaryKeyType.BINARY);
            assertTrue(!schemaA.equals(schemaB));
        }

        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.BINARY);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("A", PrimaryKeyType.STRING);
            assertTrue(!schemaA.equals(schemaB));
        }

        {
            PrimaryKeySchema schemaA = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            PrimaryKeySchema schemaB = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            assertTrue(schemaA.equals(schemaB));
        }
    }

    @Test
    public void testHashCode() {
        {
            Map<PrimaryKeySchema, String> schemaMap = new HashMap<PrimaryKeySchema, String>();
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);
            schemaMap.put(schema, "1");
        }

        {
            Map<PrimaryKeySchema, String> schemaMap = new HashMap<PrimaryKeySchema, String>();
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);
            schemaMap.put(schema, "1");
        }
    }

    @Test
    public void testToString() {
        {
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);

            String str = schema.toString();
            assertEquals("A:INTEGER", str);
        }

        {
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            String str = schema.toString();
            assertEquals("A:INTEGER:AUTO_INCREMENT", str);
        }
    }

    @Test
    public void testJsonizer() {
        {
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER);

            String json = schema.jsonize();
            assertEquals("{\"Name\": \"A\", \"Type\": \"INTEGER\"}", json);
        }

        {
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            String json = schema.jsonize();
            assertEquals("{\"Name\": \"A\", \"Type\": \"INTEGER\", \"Option\":\"AUTO_INCREMENT\"}", json);
        }

        {
            PrimaryKeySchema schema = new PrimaryKeySchema("A", PrimaryKeyType.DATETIME);

            String json = schema.jsonize();
            assertEquals("{\"Name\": \"A\", \"Type\": \"DATETIME\"}", json);
        }
    }
}
