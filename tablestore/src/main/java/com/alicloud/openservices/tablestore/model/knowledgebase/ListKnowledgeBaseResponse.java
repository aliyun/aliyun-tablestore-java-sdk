package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for listing knowledge bases.
 * <p>
 * This class encapsulates the response returned after listing all knowledge bases,
 * including pagination support and detailed information for each knowledge base.
 * </p>
 */
public class ListKnowledgeBaseResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the list of knowledge bases and pagination token.
     */
    private ListKnowledgeBaseData data;

    /**
     * Default constructor.
     */
    public ListKnowledgeBaseResponse() {
    }

    /**
     * Gets the response code.
     *
     * @return the response code
     */
    public String getCode() {
        return code;
    }

    /**
     * Sets the response code.
     *
     * @param code the response code to set
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Gets the response message.
     *
     * @return the response message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the response message.
     *
     * @param message the response message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Gets the data containing knowledge bases and pagination token.
     *
     * @return the list knowledge base data
     */
    public ListKnowledgeBaseData getData() {
        return data;
    }

    /**
     * Sets the data containing knowledge bases and pagination token.
     *
     * @param data the list knowledge base data to set
     */
    public void setData(ListKnowledgeBaseData data) {
        this.data = data;
    }

    /**
     * Data container for listing knowledge bases.
     * <p>
     * This class contains the list of knowledge bases and a pagination token
     * for retrieving the next page of results.
     * </p>
     */
    public static class ListKnowledgeBaseData {
        /**
         * The list of knowledge bases.
         */
        private List<KnowledgeBaseInfo> knowledgeBases;
        
        /**
         * The pagination token for retrieving the next page of results.
         * Null if there are no more results.
         */
        private String nextToken;

        /**
         * Default constructor.
         */
        public ListKnowledgeBaseData() {
        }

        /**
         * Gets the list of knowledge bases.
         *
         * @return the list of knowledge base information
         */
        public List<KnowledgeBaseInfo> getKnowledgeBases() {
            return knowledgeBases;
        }

        /**
         * Sets the list of knowledge bases.
         *
         * @param knowledgeBases the list of knowledge base information to set
         */
        public void setKnowledgeBases(List<KnowledgeBaseInfo> knowledgeBases) {
            this.knowledgeBases = knowledgeBases;
        }

        /**
         * Gets the pagination token for the next page.
         *
         * @return the next token, or null if there are no more results
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
    }

    /**
     * Summary information about a knowledge base.
     * <p>
     * This class contains basic information about a knowledge base,
     * including name, description, timestamps, and configuration.
     * </p>
     */
    public static class KnowledgeBaseInfo {
        /**
         * The name of the knowledge base.
         */
        private String knowledgeBaseName;
        
        /**
         * The description of the knowledge base.
         */
        private String description;
        
        /**
         * The timestamp when the knowledge base was created (Unix timestamp in milliseconds).
         */
        private Long createdAt;
        
        /**
         * The timestamp when the knowledge base was last updated (Unix timestamp in milliseconds).
         */
        private Long updatedAt;
        
        /**
         * Indicates whether subspace is enabled for this knowledge base.
         */
        private Boolean subspace;
        
        /**
         * The list of tags associated with the knowledge base.
         */
        private List<String> tags;
        
        /**
         * The list of metadata fields defined for the knowledge base.
         */
        private List<MetadataField> metadata;

        /**
         * Default constructor.
         */
        public KnowledgeBaseInfo() {
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
         * Gets the description of the knowledge base.
         *
         * @return the description
         */
        public String getDescription() {
            return description;
        }

        /**
         * Sets the description of the knowledge base.
         *
         * @param description the description to set
         */
        public void setDescription(String description) {
            this.description = description;
        }

        /**
         * Gets the creation timestamp.
         *
         * @return the creation timestamp in milliseconds
         */
        public Long getCreatedAt() {
            return createdAt;
        }

        /**
         * Sets the creation timestamp.
         *
         * @param createdAt the creation timestamp in milliseconds
         */
        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
        }

        /**
         * Gets the last update timestamp.
         *
         * @return the last update timestamp in milliseconds
         */
        public Long getUpdatedAt() {
            return updatedAt;
        }

        /**
         * Sets the last update timestamp.
         *
         * @param updatedAt the last update timestamp in milliseconds
         */
        public void setUpdatedAt(Long updatedAt) {
            this.updatedAt = updatedAt;
        }

        /**
         * Gets the subspace configuration.
         *
         * @return true if subspace is enabled, false otherwise
         */
        public Boolean getSubspace() {
            return subspace;
        }

        /**
         * Sets the subspace configuration.
         *
         * @param subspace true to enable subspace, false otherwise
         */
        public void setSubspace(Boolean subspace) {
            this.subspace = subspace;
        }

        /**
         * Gets the list of tags.
         *
         * @return the list of tags
         */
        public List<String> getTags() {
            return tags;
        }

        /**
         * Sets the list of tags.
         *
         * @param tags the list of tags to set
         */
        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        /**
         * Gets the list of metadata fields.
         *
         * @return the list of metadata fields
         */
        public List<MetadataField> getMetadata() {
            return metadata;
        }

        /**
         * Sets the list of metadata fields.
         *
         * @param metadata the list of metadata fields to set
         */
        public void setMetadata(List<MetadataField> metadata) {
            this.metadata = metadata;
        }
    }
}
