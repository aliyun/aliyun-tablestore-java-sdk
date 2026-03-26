package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.annotations.SerializedName;

/**
 * Enumeration of reranking types for retrieval results.
 * <p>
 * This enum defines the available methods for reranking search results: RRF (Reciprocal Rank Fusion), MODEL-based reranking, and WEIGHT-based reranking.
 * </p>
 */
public enum RerankingType {
    /**
     * Reciprocal Rank Fusion reranking.
     */
    @SerializedName("RRF") RRF("RRF"),

    /**
     * Model-based reranking.
     */
    @SerializedName("MODEL") MODEL("MODEL"),

    /**
     * Weight-based reranking.
     */
    @SerializedName("WEIGHT") WEIGHT("WEIGHT");

    private final String value;

    /**
     * Constructs a RerankingType with the specified value.
     *
     * @param value the string value
     */
    RerankingType(String value) {
        this.value = value;
    }

    /**
     * Gets the string value of this reranking type.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the string value of this reranking type.
     *
     * @return the string value
     */
    @Override
    public String toString() {
        return value;
    }

    /**
     * Converts a string value to a RerankingType.
     *
     * @param value the string value to convert
     * @return the corresponding RerankingType
     * @throws IllegalArgumentException if the value is null or unsupported
     */
    public static RerankingType fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("RerankingType value cannot be null");
        }
        for (RerankingType type : RerankingType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported RerankingType: " + value);
    }
}
