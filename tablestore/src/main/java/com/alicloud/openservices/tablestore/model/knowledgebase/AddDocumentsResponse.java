package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for adding documents to a knowledge base.
 * <p>
 * This class encapsulates the response returned after adding documents,
 * including the status and details of each added document.
 * </p>
 */
public class AddDocumentsResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the details of added documents.
     */
    private AddDocumentsData data;

    /**
     * Default constructor.
     */
    public AddDocumentsResponse() {
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
     * Gets the data containing added documents details.
     *
     * @return the added documents data
     */
    public AddDocumentsData getData() {
        return data;
    }

    /**
     * Sets the data containing added documents details.
     *
     * @param data the added documents data to set
     */
    public void setData(AddDocumentsData data) {
        this.data = data;
    }

    /**
     * Data container for added documents.
     * <p>
     * This class contains the list of document details for documents that were added.
     * </p>
     */
    public static class AddDocumentsData {
        /**
         * The list of document details.
         */
        private List<DocumentDetail> documentDetails;

        /**
         * Default constructor.
         */
        public AddDocumentsData() {
        }

        /**
         * Gets the list of document details.
         *
         * @return the list of document details
         */
        public List<DocumentDetail> getDocumentDetails() {
            return documentDetails;
        }

        /**
         * Sets the list of document details.
         *
         * @param documentDetails the list of document details to set
         */
        public void setDocumentDetails(List<DocumentDetail> documentDetails) {
            this.documentDetails = documentDetails;
        }
    }

    /**
     * Details of an added document.
     * <p>
     * This class contains information about a document that was added,
     * including its ID, status, OSS key, and failure reason if applicable.
     * </p>
     */
    public static class DocumentDetail {
        /**
         * The document ID.
         */
        private String docId;
        
        /**
         * The status of the document addition.
         */
        private String status;
        
        /**
         * The OSS key for the document.
         */
        private String ossKey;
        
        /**
         * The reason for failure if the document addition failed.
         */
        private String failureReason;

        /**
         * Default constructor.
         */
        public DocumentDetail() {
        }

        /**
         * Gets the document ID.
         *
         * @return the document ID
         */
        public String getDocId() {
            return docId;
        }

        /**
         * Sets the document ID.
         *
         * @param docId the document ID to set
         */
        public void setDocId(String docId) {
            this.docId = docId;
        }

        /**
         * Gets the document status.
         *
         * @return the document status
         */
        public String getStatus() {
            return status;
        }

        /**
         * Sets the document status.
         *
         * @param status the document status to set
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Gets the OSS key.
         *
         * @return the OSS key
         */
        public String getOssKey() {
            return ossKey;
        }

        /**
         * Sets the OSS key.
         *
         * @param ossKey the OSS key to set
         */
        public void setOssKey(String ossKey) {
            this.ossKey = ossKey;
        }

        /**
         * Gets the failure reason.
         *
         * @return the failure reason, or null if successful
         */
        public String getFailureReason() {
            return failureReason;
        }
        
        /**
         * Sets the failure reason.
         *
         * @param failureReason the failure reason to set
         */
        public void setFailureReason(String failureReason) {
            this.failureReason = failureReason;
        }
    }
}
