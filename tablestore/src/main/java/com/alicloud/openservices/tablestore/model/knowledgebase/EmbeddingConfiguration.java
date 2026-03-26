package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for embedding generation in a knowledge base.
 * <p>
 * This class encapsulates the configuration for generating embeddings,
 * including provider, model, dimension, URL, and API key.
 * </p>
 */
public class EmbeddingConfiguration {
    /**
     * The embedding provider (e.g., "openai", "huggingface").
     */
    private String provider;
    
    /**
     * The embedding model name.
     */
    private String model;
    
    /**
     * The dimension of the embedding vectors.
     */
    private Integer dimension;
    
    /**
     * The URL for the embedding service.
     */
    private String url;
    
    /**
     * The API key for accessing the embedding service.
     */
    private String apiKey;

    /**
     * Default constructor.
     */
    public EmbeddingConfiguration() {
    }

    /**
     * Gets the embedding provider.
     *
     * @return the provider
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the embedding provider.
     *
     * @param provider the provider to set
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Gets the embedding model name.
     *
     * @return the model name
     */
    public String getModel() {
        return model;
    }

    /**
     * Sets the embedding model name.
     *
     * @param model the model name to set
     */
    public void setModel(String model) {
        this.model = model;
    }

    /**
     * Gets the embedding dimension.
     *
     * @return the dimension
     */
    public Integer getDimension() {
        return dimension;
    }

    /**
     * Sets the embedding dimension.
     *
     * @param dimension the dimension to set
     */
    public void setDimension(Integer dimension) {
        this.dimension = dimension;
    }

    /**
     * Gets the embedding service URL.
     *
     * @return the URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the embedding service URL.
     *
     * @param url the URL to set
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Gets the API key.
     *
     * @return the API key
     */
    public String getApiKey() {
        return apiKey;
    }

    /**
     * Sets the API key.
     *
     * @param apiKey the API key to set
     */
    public void setApiKey(String apiKey) {
        this.apiKey = apiKey;
    }
}
