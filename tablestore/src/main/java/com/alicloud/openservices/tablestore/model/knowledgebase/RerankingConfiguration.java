package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for reranking retrieval results.
 * <p>
 * This class encapsulates the configuration for reranking search results,
 * including the reranking type, number of results, and specific configuration
 * based on the reranking method (RRF, MODEL, or WEIGHT).
 * </p>
 */
public class RerankingConfiguration {
    /**
     * The type of reranking to apply.
     */
    private RerankingType type;
    
    /**
     * The number of results to return after reranking.
     */
    private Integer numberOfResults;
    
    /**
     * The RRF (Reciprocal Rank Fusion) configuration.
     */
    private RRFConfiguration rrfConfiguration;
    
    /**
     * The model-based reranking configuration.
     */
    private ModelConfiguration modelConfiguration;
    
    /**
     * The weight-based reranking configuration.
     */
    private WeightConfiguration weightConfiguration;

    /**
     * Default constructor.
     */
    public RerankingConfiguration() {
    }

    /**
     * Gets the reranking type.
     *
     * @return the reranking type
     */
    public RerankingType getType() {
        return type;
    }

    /**
     * Sets the reranking type.
     *
     * @param type the reranking type to set
     */
    public void setType(RerankingType type) {
        this.type = type;
    }

    /**
     * Gets the number of results to return.
     *
     * @return the number of results
     */
    public Integer getNumberOfResults() {
        return numberOfResults;
    }

    /**
     * Sets the number of results to return.
     *
     * @param numberOfResults the number of results to set
     */
    public void setNumberOfResults(Integer numberOfResults) {
        this.numberOfResults = numberOfResults;
    }

    /**
     * Gets the RRF configuration.
     *
     * @return the RRF configuration
     */
    public RRFConfiguration getRRFConfiguration() {
        return rrfConfiguration;
    }

    /**
     * Sets the RRF configuration.
     *
     * @param rrfConfiguration the RRF configuration to set
     */
    public void setRRFConfiguration(RRFConfiguration rrfConfiguration) {
        this.rrfConfiguration = rrfConfiguration;
    }

    /**
     * Gets the model configuration.
     *
     * @return the model configuration
     */
    public ModelConfiguration getModelConfiguration() {
        return modelConfiguration;
    }

    /**
     * Sets the model configuration.
     *
     * @param modelConfiguration the model configuration to set
     */
    public void setModelConfiguration(ModelConfiguration modelConfiguration) {
        this.modelConfiguration = modelConfiguration;
    }

    /**
     * Gets the weight configuration.
     *
     * @return the weight configuration
     */
    public WeightConfiguration getWeightConfiguration() {
        return weightConfiguration;
    }

    /**
     * Sets the weight configuration.
     *
     * @param weightConfiguration the weight configuration to set
     */
    public void setWeightConfiguration(WeightConfiguration weightConfiguration) {
        this.weightConfiguration = weightConfiguration;
    }
}
