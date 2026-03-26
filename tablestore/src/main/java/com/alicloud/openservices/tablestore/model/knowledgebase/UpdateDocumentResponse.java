package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

/**
 * Response for updating a document in a knowledge base.
 * <p>
 * This class encapsulates the response returned after updating a document,
 * including the update status and document details.
 * </p>
 */
public class UpdateDocumentResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing update status and document details.
     */
    private UpdateDocumentData data;

    /**
     * Default constructor.
     */
    public UpdateDocumentResponse() {
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
     * Gets the data containing update status and document details.
     *
     * @return the update document data
     */
    public UpdateDocumentData getData() {
        return data;
    }

    /**
     * Sets the data containing update status and document details.
     *
     * @param data the update document data to set
     */
    public void setData(UpdateDocumentData data) {
        this.data = data;
    }

    /**
     * Data container for document update.
     * <p>
     * This class contains the update status and document details.
     * </p>
     */
    public static class UpdateDocumentData {
        /**
         * The status of the document update.
         */
        private String updateStatus;
        
        /**
         * The document ID.
         */
        private String docId;
        
        /**
         * The OSS key for the document.
         */
        private String ossKey;
        
        /**
         * The timestamp when the document was last updated (Unix timestamp in milliseconds).
         */
        private Long updatedAt;

        /**
         * Default constructor.
         */
        public UpdateDocumentData() {
        }

        /**
         * Gets the update status.
         *
         * @return the update status
         */
        public String getUpdateStatus() {
            return updateStatus;
        }

        /**
         * Sets the update status.
         *
         * @param updateStatus the update status to set
         */
        public void setUpdateStatus(String updateStatus) {
            this.updateStatus = updateStatus;
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
         * Gets the last update timestamp.
         *
         * @return the last update timestamp in milliseconds
         */
        public Long getUpdatedAt() {
            return updatedAt;
        }

        /**
         * Sets the last update timestamp.
         *
         * @param updatedAt the last update timestamp in milliseconds
         */
        public void setUpdatedAt(Long updatedAt) {
            this.updatedAt = updatedAt;
        }
    }
}
