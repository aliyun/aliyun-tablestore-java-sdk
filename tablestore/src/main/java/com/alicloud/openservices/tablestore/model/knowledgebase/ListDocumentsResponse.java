package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for listing documents in a knowledge base.
 * <p>
 * This class encapsulates the response returned after listing documents,
 * including pagination support and detailed information for each document.
 * </p>
 */
public class ListDocumentsResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the list of documents and pagination token.
     */
    private ListDocumentsData data;

    /**
     * Default constructor.
     */
    public ListDocumentsResponse() {
    }

    /**
     * Gets the response code.
     *
     * @return the response code
     */
    public String getCode() {
        return code;
    }

    /**
     * Sets the response code.
     *
     * @param code the response code to set
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * Gets the response message.
     *
     * @return the response message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Sets the response message.
     *
     * @param message the response message to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * Gets the data containing documents and pagination token.
     *
     * @return the list documents data
     */
    public ListDocumentsData getData() {
        return data;
    }

    /**
     * Sets the data containing documents and pagination token.
     *
     * @param data the list documents data to set
     */
    public void setData(ListDocumentsData data) {
        this.data = data;
    }

    /**
     * Data container for listing documents.
     * <p>
     * This class contains the list of document details and a pagination token
     * for retrieving the next page of results.
     * </p>
     */
    public static class ListDocumentsData {
        /**
         * The list of document details.
         */
        private List<DocumentInfo> documentDetails;
        
        /**
         * The pagination token for retrieving the next page of results.
         * Null if there are no more results.
         */
        private String nextToken;

        /**
         * Default constructor.
         */
        public ListDocumentsData() {
        }

        /**
         * Gets the list of document details.
         *
         * @return the list of document information
         */
        public List<DocumentInfo> getDocumentDetails() {
            return documentDetails;
        }

        /**
         * Sets the list of document details.
         *
         * @param documentDetails the list of document information to set
         */
        public void setDocumentDetails(List<DocumentInfo> documentDetails) {
            this.documentDetails = documentDetails;
        }

        /**
         * Gets the pagination token for the next page.
         *
         * @return the next token, or null if there are no more results
         */
        public String getNextToken() {
            return nextToken;
        }

        /**
         * Sets the pagination token for the next page.
         *
         * @param nextToken the next token to set
         */
        public void setNextToken(String nextToken) {
            this.nextToken = nextToken;
        }
    }
}
