package com.alicloud.openservices.tablestore.model.knowledgebase;

import java.util.List;

/**
 * Configuration for retrieval operations in a knowledge base.
 * <p>
 * This class encapsulates the complete configuration for retrieving documents,
 * including search types, search configurations, reranking, and metadata filtering.
 * </p>
 */
public class RetrievalConfiguration {
    /**
     * The list of search types to use for retrieval.
     */
    private List<SearchType> searchType;
    
    /**
     * The configuration for dense vector search.
     */
    private DenseVectorSearchConfiguration denseVectorSearchConfiguration;
    
    /**
     * The configuration for full-text search.
     */
    private FullTextSearchConfiguration fullTextSearchConfiguration;
    
    /**
     * The configuration for reranking results.
     */
    private RerankingConfiguration rerankingConfiguration;
    
    /**
     * The metadata filter to apply during retrieval.
     */
    private MetadataFilter filter;

    /**
     * Default constructor.
     */
    public RetrievalConfiguration() {
    }

    /**
     * Gets the list of search types.
     *
     * @return the list of search types
     */
    public List<SearchType> getSearchType() {
        return searchType;
    }

    /**
     * Sets the list of search types.
     *
     * @param searchType the list of search types to set
     */
    public void setSearchType(List<SearchType> searchType) {
        this.searchType = searchType;
    }

    /**
     * Gets the dense vector search configuration.
     *
     * @return the dense vector search configuration
     */
    public DenseVectorSearchConfiguration getDenseVectorSearchConfiguration() {
        return denseVectorSearchConfiguration;
    }

    /**
     * Sets the dense vector search configuration.
     *
     * @param denseVectorSearchConfiguration the dense vector search configuration to set
     */
    public void setDenseVectorSearchConfiguration(DenseVectorSearchConfiguration denseVectorSearchConfiguration) {
        this.denseVectorSearchConfiguration = denseVectorSearchConfiguration;
    }

    /**
     * Gets the full-text search configuration.
     *
     * @return the full-text search configuration
     */
    public FullTextSearchConfiguration getFullTextSearchConfiguration() {
        return fullTextSearchConfiguration;
    }

    /**
     * Sets the full-text search configuration.
     *
     * @param fullTextSearchConfiguration the full-text search configuration to set
     */
    public void setFullTextSearchConfiguration(FullTextSearchConfiguration fullTextSearchConfiguration) {
        this.fullTextSearchConfiguration = fullTextSearchConfiguration;
    }

    /**
     * Gets the reranking configuration.
     *
     * @return the reranking configuration
     */
    public RerankingConfiguration getRerankingConfiguration() {
        return rerankingConfiguration;
    }

    /**
     * Sets the reranking configuration.
     *
     * @param rerankingConfiguration the reranking configuration to set
     */
    public void setRerankingConfiguration(RerankingConfiguration rerankingConfiguration) {
        this.rerankingConfiguration = rerankingConfiguration;
    }

    /**
     * Gets the metadata filter.
     *
     * @return the metadata filter
     */
    public MetadataFilter getFilter() {
        return filter;
    }

    /**
     * Sets the metadata filter.
     *
     * @param filter the metadata filter to set
     */
    public void setFilter(MetadataFilter filter) {
        this.filter = filter;
    }
}
