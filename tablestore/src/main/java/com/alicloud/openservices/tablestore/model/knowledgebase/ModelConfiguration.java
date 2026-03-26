package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for a model used in knowledge base operations.
 * <p>
 * This class encapsulates the basic configuration for a model,
 * including provider and model name.
 * </p>
 */
public class ModelConfiguration {
    /**
     * The model provider (e.g., "openai", "anthropic").
     */
    private String provider;
    
    /**
     * The model name.
     */
    private String model;

    /**
     * Default constructor.
     */
    public ModelConfiguration() {
    }

    /**
     * Gets the model provider.
     *
     * @return the provider
     */
    public String getProvider() {
        return provider;
    }

    /**
     * Sets the model provider.
     *
     * @param provider the provider to set
     */
    public void setProvider(String provider) {
        this.provider = provider;
    }

    /**
     * Gets the model name.
     *
     * @return the model name
     */
    public String getModel() {
        return model;
    }

    /**
     * Sets the model name.
     *
     * @param model the model name to set
     */
    public void setModel(String model) {
        this.model = model;
    }
}
