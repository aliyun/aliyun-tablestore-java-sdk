package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * Request class for listing chunks in a knowledge base.
 * <p>
 * This class represents a request to list document chunks from a knowledge base.
 * You can filter chunks by subspace, document ID, or OSS key, and use pagination
 * parameters to control the result set size.
 * </p>
 */
public class ListChunksRequest implements Request {

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
     * Document ID to filter chunks.
     */
    private String docId;

    /**
     * OSS key to filter chunks.
     */
    private String ossKey;

    /**
     * Maximum number of results to return.
     */
    private Integer maxResults;

    /**
     * Token for pagination, used to retrieve the next page of results.
     */
    private String nextToken;

    /**
     * Constructs a new ListChunksRequest.
     */
    public ListChunksRequest() {
    }

    /**
     * Constructs a new ListChunksRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base
     */
    public ListChunksRequest(String knowledgeBaseName) {
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
     * Gets the maximum number of results to return.
     *
     * @return the maximum number of results
     */
    public Integer getMaxResults() {
        return maxResults;
    }

    /**
     * Sets the maximum number of results to return.
     *
     * @param maxResults the maximum number of results
     */
    public void setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
    }

    /**
     * Gets the pagination token for retrieving the next page of results.
     *
     * @return the next token
     */
    public String getNextToken() {
        return nextToken;
    }

    /**
     * Sets the pagination token for retrieving the next page of results.
     *
     * @param nextToken the next token
     */
    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
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
        return OperationNames.OP_LIST_CHUNKS;
    }

    /**
     * Converts this request to JSON string.
     * <p>
     * If a custom JSON string is set via {@link #setJsonStr(String)}, it will be returned.
     * Otherwise, this object will be serialized using Gson.
     * </p>
     *
     * @return the JSON representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }
}
