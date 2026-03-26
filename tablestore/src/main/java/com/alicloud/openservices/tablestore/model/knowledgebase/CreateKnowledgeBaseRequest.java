package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

/**
 * Request for creating a knowledge base.
 * <p>
 * This class encapsulates all parameters required to create a new knowledge base,
 * including name, description, subspace configuration, tags, metadata fields,
 * embedding configuration, and retrieval configuration.
 * </p>
 */
public class CreateKnowledgeBaseRequest implements Request {

    /**
     * The JSON string representation of this request.
     * This field is transient and will not be serialized.
     */
    private transient String jsonStr;
    
    /**
     * The name of the knowledge base to create.
     */
    private String knowledgeBaseName;
    
    /**
     * The description of the knowledge base.
     */
    private String description;
    
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
    public CreateKnowledgeBaseRequest() {
    }

    /**
     * Constructs a CreateKnowledgeBaseRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base to create
     */
    public CreateKnowledgeBaseRequest(String knowledgeBaseName) {
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
     * @return the operation name "CreateKnowledgeBase"
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_KNOWLEDGE_BASE;
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
