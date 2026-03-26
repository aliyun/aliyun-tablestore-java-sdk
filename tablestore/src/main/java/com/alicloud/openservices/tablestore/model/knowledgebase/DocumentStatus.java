package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.google.gson.annotations.SerializedName;

/**
 * Enumeration of document status in a knowledge base.
 * <p>
 * This enum defines the possible states of a document during its lifecycle
 * in the knowledge base, from pending to completion or failure.
 * </p>
 */
public enum DocumentStatus {
    /**
     * The document is pending processing.
     */
    @SerializedName("PENDING")
    PENDING("PENDING"),

    /**
     * The document is currently being indexed.
     */
    @SerializedName("INDEXING")
    INDEXING("INDEXING"),

    /**
     * The document has been successfully processed and indexed.
     */
    @SerializedName("COMPLETED")
    COMPLETED("COMPLETED"),

    /**
     * The document is being deleted.
     */
    @SerializedName("DELETING")
    DELETING("DELETING"),

    /**
     * The document processing has failed.
     */
    @SerializedName("FAILED")
    FAILED("FAILED");

    private final String value;

    /**
     * Constructs a DocumentStatus with the specified value.
     *
     * @param value the string value
     */
    DocumentStatus(String value) {
        this.value = value;
    }

    /**
     * Gets the string value of this document status.
     *
     * @return the string value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the string value of this document status.
     *
     * @return the string value
     */
    @Override
    public String toString() {
        return value;
    }

    /**
     * Converts a string value to a DocumentStatus.
     *
     * @param value the string value to convert
     * @return the corresponding DocumentStatus
     * @throws IllegalArgumentException if the value is null or unsupported
     */
    public static DocumentStatus fromValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("DocumentStatus value cannot be null");
        }
        for (DocumentStatus status : DocumentStatus.values()) {
            if (status.value.equals(value)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unsupported DocumentStatus: " + value);
    }
}
