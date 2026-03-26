package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for dense vector search.
 * <p>
 * This class encapsulates the configuration for performing dense vector
 * similarity search in a knowledge base.
 * </p>
 */
public class DenseVectorSearchConfiguration {
    /**
     * The number of results to return from dense vector search.
     */
    private Integer numberOfResults;

    /**
     * Default constructor.
     */
    public DenseVectorSearchConfiguration() {
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
}
