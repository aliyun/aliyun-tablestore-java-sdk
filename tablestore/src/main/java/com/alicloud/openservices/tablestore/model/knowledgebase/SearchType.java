package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.annotations.SerializedName;

/**
 * Enumeration of search types for retrieval operations.
 * <p>
 * This enum defines the available search methods for retrieving documents: DENSE_VECTOR for vector similarity search and FULL_TEXT for text-based search.
 * </p>
 */
public enum SearchType {
    /**
     * Dense vector similarity search.
     */
    @SerializedName("DENSE_VECTOR") DENSE_VECTOR("DENSE_VECTOR"),

    /**
     * Full-text search.
     */
    @SerializedName("FULL_TEXT") FULL_TEXT("FULL_TEXT");

    private final String value;

    /**
     * Constructs a SearchType with the specified value.
     *
     * @param value the string value
     */
    SearchType(String value) {
        this.value = value;
    }

    /**
     * Gets the string value of this search type.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the string value of this search type.
     *
     * @return the string value
     */
    @Override
    public String toString() {
        return value;
    }

    /**
     * Converts a string value to a SearchType.
     *
     * @param value the string value to convert
     * @return the corresponding SearchType
     * @throws IllegalArgumentException if the value is null or unsupported
     */
    public static SearchType fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("SearchType value cannot be null");
        }
        for (SearchType type : SearchType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported SearchType: " + value);
    }
}
