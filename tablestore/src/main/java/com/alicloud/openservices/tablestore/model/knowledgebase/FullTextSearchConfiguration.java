package com.alicloud.openservices.tablestore.model.knowledgebase;

/**
 * Configuration for full-text search.
 * <p>
 * This class encapsulates the configuration for performing full-text
 * search in a knowledge base.
 * </p>
 */
public class FullTextSearchConfiguration {
    /**
     * The number of results to return from full-text search.
     */
    private Integer numberOfResults;

    /**
     * Default constructor.
     */
    public FullTextSearchConfiguration() {
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
