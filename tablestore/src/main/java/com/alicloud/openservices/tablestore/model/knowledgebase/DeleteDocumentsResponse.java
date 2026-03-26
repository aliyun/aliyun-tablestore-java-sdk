package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for deleting documents from a knowledge base.
 * <p>
 * This class encapsulates the response returned after deleting documents,
 * including the status and details of each deleted document.
 * </p>
 */
public class DeleteDocumentsResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the details of deleted documents.
     */
    private DeleteDocumentsData data;

    /**
     * Default constructor.
     */
    public DeleteDocumentsResponse() {
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
     * Gets the data containing deleted documents details.
     *
     * @return the deleted documents data
     */
    public DeleteDocumentsData getData() {
        return data;
    }

    /**
     * Sets the data containing deleted documents details.
     *
     * @param data the deleted documents data to set
     */
    public void setData(DeleteDocumentsData data) {
        this.data = data;
    }

    /**
     * Data container for deleted documents.
     * <p>
     * This class contains the list of document details for documents that were deleted.
     * </p>
     */
    public static class DeleteDocumentsData {
        /**
         * The list of document details.
         */
        private List<DocumentDetail> documentDetails;

        /**
         * Default constructor.
         */
        public DeleteDocumentsData() {
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
     * Details of a deleted document.
     * <p>
     * This class contains information about a document that was deleted,
     * including its ID, status, OSS key, and failure reason if applicable.
     * </p>
     */
    public static class DocumentDetail {
        /**
         * The status of the document deletion.
         */
        private String status;
        
        /**
         * The OSS key for the document.
         */
        private String ossKey;
        
        /**
         * The document ID.
         */
        private String docId;
        
        /**
         * The reason for failure if the document deletion failed.
         */
        private String failureReason;

        /**
         * Default constructor.
         */
        public DocumentDetail() {
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
