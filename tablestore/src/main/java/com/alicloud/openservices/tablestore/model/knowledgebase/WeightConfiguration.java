package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for weighted retrieval results.
 * <p>
 * This class encapsulates the weights for different search methods
 * when combining retrieval results using weighted scoring.
 * </p>
 */
public class WeightConfiguration {
    /**
     * The weight for dense vector search results.
     */
    private Double denseVectorSearchWeight;
    
    /**
     * The weight for full-text search results.
     */
    private Double fullTextSearchWeight;

    /**
     * Default constructor.
     */
    public WeightConfiguration() {
    }

    /**
     * Gets the dense vector search weight.
     *
     * @return the dense vector search weight
     */
    public Double getDenseVectorSearchWeight() {
        return denseVectorSearchWeight;
    }

    /**
     * Sets the dense vector search weight.
     *
     * @param denseVectorSearchWeight the dense vector search weight to set
     */
    public void setDenseVectorSearchWeight(Double denseVectorSearchWeight) {
        this.denseVectorSearchWeight = denseVectorSearchWeight;
    }

    /**
     * Gets the full-text search weight.
     *
     * @return the full-text search weight
     */
    public Double getFullTextSearchWeight() {
        return fullTextSearchWeight;
    }

    /**
     * Sets the full-text search weight.
     *
     * @param fullTextSearchWeight the full-text search weight to set
     */
    public void setFullTextSearchWeight(Double fullTextSearchWeight) {
        this.fullTextSearchWeight = fullTextSearchWeight;
    }
}
