package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;

/**
 * Request for retrieving documents from a knowledge base.
 * <p>
 * This class encapsulates the parameters required to perform a retrieval operation
 * on a knowledge base, including the query, subspace filters, and retrieval configuration.
 * </p>
 */
public class RetrieveRequest implements Request {

    /**
     * The JSON string representation of this request.
     * This field is transient and will not be serialized.
     */
    private transient String jsonStr;
    
    /**
     * The name of the knowledge base to retrieve from.
     */
    private String knowledgeBaseName;
    
    /**
     * The list of subspaces to filter the retrieval.
     */
    private List<String> subspace;
    
    /**
     * The retrieval query containing the text and type.
     */
    private RetrievalQuery retrievalQuery;
    
    /**
     * The retrieval configuration for this request.
     */
    private RetrievalConfiguration retrievalConfiguration;

    /**
     * Default constructor.
     */
    public RetrieveRequest() {
    }

    /**
     * Constructs a RetrieveRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base to retrieve from
     */
    public RetrieveRequest(String knowledgeBaseName) {
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
     * Gets the list of subspaces to filter.
     *
     * @return the list of subspaces
     */
    public List<String> getSubspace() {
        return subspace;
    }

    /**
     * Sets the list of subspaces to filter.
     *
     * @param subspace the list of subspaces to set
     */
    public void setSubspace(List<String> subspace) {
        this.subspace = subspace;
    }

    /**
     * Gets the retrieval query.
     *
     * @return the retrieval query
     */
    public RetrievalQuery getRetrievalQuery() {
        return retrievalQuery;
    }

    /**
     * Sets the retrieval query.
     *
     * @param retrievalQuery the retrieval query to set
     */
    public void setRetrievalQuery(RetrievalQuery retrievalQuery) {
        this.retrievalQuery = retrievalQuery;
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
     * @return the operation name "Retrieve"
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_RETRIEVE;
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

    /**
     * The retrieval query containing text and type information.
     * <p>
     * This class encapsulates the query text and type for retrieval operations.
     * </p>
     */
    public static class RetrievalQuery {

        public static final String QUERY_TYPE_TEXT = "TEXT";

        /**
         * The query text for retrieval.
         */
        private String text;
        
        /**
         * The type of the retrieval query.
         */
        private String type;

        /**
         * Default constructor.
         */
        public RetrievalQuery() {
        }

        /**
         * Constructs a RetrievalQuery with the specified text.
         *
         * @param text the query text
         */
        public RetrievalQuery(String text) {
            this.text = text;
        }

        /**
         * Gets the query text.
         *
         * @return the query text
         */
        public String getText() {
            return text;
        }

        /**
         * Sets the query text.
         *
         * @param text the query text to set
         */
        public void setText(String text) {
            this.text = text;
        }

        /**
         * Gets the query type.
         *
         * @return the query type
         */
        public String getType() {
            return type;
        }

        /**
         * Sets the query type.
         *
         * @param type the query type to set
         */
        public void setType(String type) {
            this.type = type;
        }
    }

}
