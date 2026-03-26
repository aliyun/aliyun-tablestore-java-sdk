package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for describing a knowledge base.
 * <p>
 * This class encapsulates the response returned after describing a knowledge base,
 * including the detailed configuration and metadata.
 * </p>
 */
public class DescribeKnowledgeBaseResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The detailed information about the knowledge base.
     */
    private KnowledgeBaseDetail data;

    /**
     * Default constructor.
     */
    public DescribeKnowledgeBaseResponse() {
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
     * Gets the detailed information about the knowledge base.
     *
     * @return the knowledge base detail
     */
    public KnowledgeBaseDetail getData() {
        return data;
    }

    /**
     * Sets the detailed information about the knowledge base.
     *
     * @param data the knowledge base detail to set
     */
    public void setData(KnowledgeBaseDetail data) {
        this.data = data;
    }

    /**
     * Detailed information about a knowledge base.
     * <p>
     * This class contains all configuration and metadata for a knowledge base,
     * including name, description, timestamps, subspace configuration, tags,
     * metadata fields, embedding configuration, and retrieval configuration.
     * </p>
     */
    public static class KnowledgeBaseDetail {
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
         * The embedding configuration for the knowledge base.
         */
        private EmbeddingConfiguration embeddingConfiguration;
        
        /**
         * The retrieval configuration for the knowledge base.
         */
        private RetrievalConfiguration retrievalConfiguration;

        /**
         * Default constructor.
         */
        public KnowledgeBaseDetail() {
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

        /**
         * Gets the embedding configuration.
         *
         * @return the embedding configuration
         */
        public EmbeddingConfiguration getEmbeddingConfiguration() {
            return embeddingConfiguration;
        }

        /**
         * Sets the embedding configuration.
         *
         * @param embeddingConfiguration the embedding configuration to set
         */
        public void setEmbeddingConfiguration(EmbeddingConfiguration embeddingConfiguration) {
            this.embeddingConfiguration = embeddingConfiguration;
        }

        /**
         * Gets the retrieval configuration.
         *
         * @return the retrieval configuration
         */
        public RetrievalConfiguration getRetrievalConfiguration() {
            return retrievalConfiguration;
        }

        /**
         * Sets the retrieval configuration.
         *
         * @param retrievalConfiguration the retrieval configuration to set
         */
        public void setRetrievalConfiguration(RetrievalConfiguration retrievalConfiguration) {
            this.retrievalConfiguration = retrievalConfiguration;
        }
    }

}
