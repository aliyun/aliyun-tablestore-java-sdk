package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for Reciprocal Rank Fusion (RRF) reranking.
 * <p>
 * This class encapsulates the configuration for RRF reranking,
 * which combines multiple ranked lists of results using a weighted scoring method.
 * </p>
 */
public class RRFConfiguration {
    /**
     * The weight for dense vector search results in RRF scoring.
     */
    private Double denseVectorSearchWeight;
    
    /**
     * The weight for full-text search results in RRF scoring.
     */
    private Double fullTextSearchWeight;
    
    /**
     * The constant k used in the RRF formula.
     * This parameter controls the influence of rank position on the score.
     */
    private Integer k;

    /**
     * Default constructor.
     */
    public RRFConfiguration() {
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

    /**
     * Gets the constant k for RRF scoring.
     *
     * @return the constant k
     */
    public Integer getK() {
        return k;
    }

    /**
     * Sets the constant k for RRF scoring.
     *
     * @param k the constant k to set
     */
    public void setK(Integer k) {
        this.k = k;
    }
}
