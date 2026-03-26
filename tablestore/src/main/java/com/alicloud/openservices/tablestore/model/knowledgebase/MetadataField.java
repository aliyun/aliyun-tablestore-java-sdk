package com.alicloud.openservices.tablestore.model.knowledgebase;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata field definition for a knowledge base.
 * <p>
 * This class represents a metadata field that can be defined for documents
 * in a knowledge base, including the field name and type.
 * </p>
 *
 * <p>Supported types:</p>
 * <ul>
 *   <li>Basic types: {@code string}, {@code long}, {@code double}, {@code boolean}, {@code date}</li>
 *   <li>List types: {@code string_list}, {@code long_list}, {@code double_list}, {@code boolean_list}, {@code date_list}</li>
 * </ul>
 *
 * <p>Usage examples:</p>
 * <pre>{@code
 * // Using static factory methods (recommended)
 * MetadataField author = MetadataField.stringType("author");
 * MetadataField year = MetadataField.longType("year");
 * MetadataField tags = MetadataField.stringList("tags");
 *
 * // Using constructor (still supported)
 * MetadataField field = new MetadataField("author", MetadataField.TYPE_STRING);
 * }</pre>
 */
public class MetadataField implements Serializable {
    private static final long serialVersionUID = 1L;

    // Basic type constants
    public static final String TYPE_STRING = "string";
    public static final String TYPE_LONG = "long";
    public static final String TYPE_DOUBLE = "double";
    public static final String TYPE_BOOLEAN = "boolean";
    public static final String TYPE_DATE = "date";

    // List type constants
    public static final String TYPE_STRING_LIST = "string_list";
    public static final String TYPE_LONG_LIST = "long_list";
    public static final String TYPE_DOUBLE_LIST = "double_list";
    public static final String TYPE_BOOLEAN_LIST = "boolean_list";
    public static final String TYPE_DATE_LIST = "date_list";

    /**
     * The name of the metadata field.
     */
    private String name;

    /**
     * The type of the metadata field.
     */
    private String type;

    /**
     * Default constructor.
     */
    public MetadataField() {
    }

    /**
     * Constructs a MetadataField with the specified name and type.
     *
     * @param name the field name
     * @param type the field type (use TYPE_* constants or static factory methods)
     */
    public MetadataField(String name, String type) {
        this.name = name;
        this.type = type;
    }

    // ==================== Static factory methods for basic types ====================

    /**
     * Creates a string type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with string type
     */
    public static MetadataField stringType(String name) {
        return new MetadataField(name, TYPE_STRING);
    }

    /**
     * Creates a long type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with long type
     */
    public static MetadataField longType(String name) {
        return new MetadataField(name, TYPE_LONG);
    }

    /**
     * Creates a double type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with double type
     */
    public static MetadataField doubleType(String name) {
        return new MetadataField(name, TYPE_DOUBLE);
    }

    /**
     * Creates a boolean type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with boolean type
     */
    public static MetadataField booleanType(String name) {
        return new MetadataField(name, TYPE_BOOLEAN);
    }

    /**
     * Creates a date type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with date type
     */
    public static MetadataField dateType(String name) {
        return new MetadataField(name, TYPE_DATE);
    }

    // ==================== Static factory methods for list types ====================

    /**
     * Creates a string list type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with string_list type
     */
    public static MetadataField stringList(String name) {
        return new MetadataField(name, TYPE_STRING_LIST);
    }

    /**
     * Creates a long list type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with long_list type
     */
    public static MetadataField longList(String name) {
        return new MetadataField(name, TYPE_LONG_LIST);
    }

    /**
     * Creates a double list type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with double_list type
     */
    public static MetadataField doubleList(String name) {
        return new MetadataField(name, TYPE_DOUBLE_LIST);
    }

    /**
     * Creates a boolean list type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with boolean_list type
     */
    public static MetadataField booleanList(String name) {
        return new MetadataField(name, TYPE_BOOLEAN_LIST);
    }

    /**
     * Creates a date list type metadata field.
     *
     * @param name the field name
     * @return a new MetadataField with date_list type
     */
    public static MetadataField dateList(String name) {
        return new MetadataField(name, TYPE_DATE_LIST);
    }

    // ==================== Getters and Setters ====================

    /**
     * Gets the field name.
     *
     * @return the field name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the field name.
     *
     * @param name the field name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the field type.
     *
     * @return the field type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the field type.
     *
     * @param type the field type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Checks if this MetadataField is equal to another object.
     *
     * @param o the object to compare with
     * @return true if equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetadataField that = (MetadataField) o;
        return Objects.equals(name, that.name) && Objects.equals(type, that.type);
    }

    /**
     * Computes the hash code for this MetadataField.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    /**
     * Returns a string representation of this MetadataField.
     *
     * @return the string representation
     */
    @Override
    public String toString() {
        return "MetadataField{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
