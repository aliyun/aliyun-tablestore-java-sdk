package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * Request class for listing knowledge bases.
 * <p>
 * This class represents a request to list knowledge bases with pagination support.
 * Use {@code maxResults} to specify the maximum number of results to return,
 * and {@code nextToken} for pagination.
 * </p>
 */
public class ListKnowledgeBaseRequest implements Request {

    /**
     * Custom JSON string. If set, this will be used instead of auto-serialization.
     */
    private transient String jsonStr;

    /**
     * Maximum number of results to return.
     */
    private Integer maxResults;

    /**
     * Token for pagination, used to retrieve the next page of results.
     */
    private String nextToken;

    /**
     * Constructs a new ListKnowledgeBaseRequest.
     */
    public ListKnowledgeBaseRequest() {
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
        return OperationNames.OP_LIST_KNOWLEDGE_BASE;
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
