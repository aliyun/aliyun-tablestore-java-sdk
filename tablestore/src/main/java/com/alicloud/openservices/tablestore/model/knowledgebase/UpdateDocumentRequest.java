package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.Map;

/**
 * Request class for updating a document in a knowledge base.
 * <p>
 * This class represents a request to update an existing document in a knowledge base. You can update the document's metadata and other properties.
 * </p>
 */
public class UpdateDocumentRequest implements Request {

    /**
     * Custom JSON string. If set, this will be used instead of auto-serialization.
     */
    private transient String jsonStr;

    /**
     * Name of the knowledge base.
     */
    private String knowledgeBaseName;

    /**
     * Subspace within the knowledge base.
     */
    private String subspace;

    /**
     * OSS key for the document.
     */
    private String ossKey;

    /**
     * Document ID.
     */
    private String docId;

    /**
     * Document metadata.
     */
    private Map<String, Object> metadata;

    /**
     * Tracks whether {@link #setMetadata(Map)} has been explicitly called.
     * <p>
     * This flag distinguishes between "metadata was not set" (field absent from JSON) and "metadata was explicitly set to null" (field present as {@code null}
     * in JSON, indicating deletion on the server side).
     * </p>
     */
    private transient boolean metadataExplicitlySet = false;

    /**
     * Constructs a new UpdateDocumentRequest.
     */
    public UpdateDocumentRequest() {
    }

    /**
     * Constructs a new UpdateDocumentRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base
     */
    public UpdateDocumentRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the knowledge base name.
     *
     * @return the knowledge base name
     */
    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    /**
     * Sets the knowledge base name.
     *
     * @param knowledgeBaseName the knowledge base name
     */
    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the subspace.
     *
     * @return the subspace
     */
    public String getSubspace() {
        return subspace;
    }

    /**
     * Sets the subspace.
     *
     * @param subspace the subspace
     */
    public void setSubspace(String subspace) {
        this.subspace = subspace;
    }

    /**
     * Gets the OSS key.
     *
     * @return the OSS key
     */
    public String getOssKey() {
        return ossKey;
    }

    /**
     * Sets the OSS key.
     *
     * @param ossKey the OSS key
     */
    public void setOssKey(String ossKey) {
        this.ossKey = ossKey;
    }

    /**
     * Gets the document ID.
     *
     * @return the document ID
     */
    public String getDocId() {
        return docId;
    }

    /**
     * Sets the document ID.
     *
     * @param docId the document ID
     */
    public void setDocId(String docId) {
        this.docId = docId;
    }

    /**
     * Gets the document metadata.
     *
     * @return the metadata map
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Sets the document metadata.
     * <p>
     * Setting metadata to a non-null map will update the metadata on the server. Setting metadata to {@code null} explicitly will delete the metadata on the
     * server. Not calling this method at all will leave the metadata unchanged.
     * </p>
     *
     * @param metadata the metadata map, or {@code null} to delete metadata
     */
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        this.metadataExplicitlySet = true;
    }

    /**
     * Gets the custom JSON string.
     *
     * @return the custom JSON string
     */
    public String getJsonStr() {
        return jsonStr;
    }

    /**
     * Sets the custom JSON string. If set, this will be used instead of auto-serialization.
     *
     * @param jsonStr the custom JSON string
     */
    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    /**
     * Gets the operation name for this request.
     *
     * @return the operation name
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_DOCUMENT;
    }

    /**
     * Converts this request to JSON string.
     * <p>
     * If a custom JSON string is set via {@link #setJsonStr(String)}, it will be returned. Otherwise, this object will be serialized using Gson.
     * </p>
     * <p>
     * When {@link #setMetadata(Map)} is called with {@code null}, the resulting JSON will contain {@code "metadata": null} to signal metadata deletion on the
     * server. When {@link #setMetadata(Map)} is not called at all, the metadata field will be absent from the JSON, leaving the server-side metadata
     * unchanged.
     * </p>
     *
     * @return the JSON representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        if (metadataExplicitlySet && metadata == null) {
            JsonObject jsonObject = new JsonObject();
            if (knowledgeBaseName != null) {
                jsonObject.addProperty("knowledgeBaseName", knowledgeBaseName);
            }
            if (subspace != null) {
                jsonObject.addProperty("subspace", subspace);
            }
            if (ossKey != null) {
                jsonObject.addProperty("ossKey", ossKey);
            }
            if (docId != null) {
                jsonObject.addProperty("docId", docId);
            }
            jsonObject.add("metadata", JsonNull.INSTANCE);
            return jsonObject.toString();
        }
        return GsonUtils.toJson(this);
    }
}
