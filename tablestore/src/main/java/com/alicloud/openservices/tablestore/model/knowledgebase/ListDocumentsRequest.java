package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

/**
 * Request for listing documents in a knowledge base.
 * <p>
 * This class encapsulates the parameters required to list documents,
 * including pagination support and subspace filtering.
 * </p>
 */
public class ListDocumentsRequest implements Request {

    /**
     * The JSON string representation of this request.
     * This field is transient and will not be serialized.
     */
    private transient String jsonStr;
    
    /**
     * The name of the knowledge base.
     */
    private String knowledgeBaseName;
    
    /**
     * The list of subspaces to filter documents.
     */
    private List<String> subspace;
    
    /**
     * The maximum number of results to return.
     */
    private Integer maxResults;
    
    /**
     * The pagination token for retrieving the next page of results.
     */
    private String nextToken;

    /**
     * Default constructor.
     */
    public ListDocumentsRequest() {
    }

    /**
     * Constructs a ListDocumentsRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base
     */
    public ListDocumentsRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the name of the knowledge base.
     *
     * @return the knowledge base name
     */
    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    /**
     * Sets the name of the knowledge base.
     *
     * @param knowledgeBaseName the knowledge base name to set
     */
    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the list of subspaces to filter.
     *
     * @return the list of subspaces
     */
    public List<String> getSubspace() {
        return subspace;
    }

    /**
     * Sets the list of subspaces to filter.
     *
     * @param subspace the list of subspaces to set
     */
    public void setSubspace(List<String> subspace) {
        this.subspace = subspace;
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
     * @param maxResults the maximum number of results to set
     */
    public void setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
    }

    /**
     * Gets the pagination token for the next page.
     *
     * @return the next token
     */
    public String getNextToken() {
        return nextToken;
    }

    /**
     * Sets the pagination token for the next page.
     *
     * @param nextToken the next token to set
     */
    public void setNextToken(String nextToken) {
        this.nextToken = nextToken;
    }

    /**
     * Gets the JSON string representation of this request.
     *
     * @return the JSON string
     */
    public String getJsonStr() {
        return jsonStr;
    }

    /**
     * Sets the JSON string representation of this request.
     *
     * @param jsonStr the JSON string to set
     */
    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    /**
     * Gets the operation name for this request.
     *
     * @return the operation name "ListDocuments"
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_DOCUMENTS;
    }

    /**
     * Converts this request to a JSON string.
     *
     * @return the JSON string representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        return GsonUtils.toJson(this);
    }
}
