package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for updating chunks in a knowledge base.
 * <p>
 * This class encapsulates the response returned after updating document chunks,
 * including the status and details of each updated chunk.
 * </p>
 */
public class UpdateChunksResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the details of updated chunks.
     */
    private UpdateChunksData data;

    /**
     * Default constructor.
     */
    public UpdateChunksResponse() {
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
     * Gets the data containing chunk details.
     *
     * @return the update chunks data
     */
    public UpdateChunksData getData() {
        return data;
    }

    /**
     * Sets the data containing chunk details.
     *
     * @param data the update chunks data to set
     */
    public void setData(UpdateChunksData data) {
        this.data = data;
    }

    /**
     * Data container for updated chunks.
     * <p>
     * This class contains the list of update details for chunks that were updated.
     * </p>
     */
    public static class UpdateChunksData {
        /**
         * The list of update details.
         */
        private List<UpdateChunkDetail> updateDetails;

        /**
         * Default constructor.
         */
        public UpdateChunksData() {
        }

        /**
         * Gets the list of update details.
         *
         * @return the list of update details
         */
        public List<UpdateChunkDetail> getUpdateDetails() {
            return updateDetails;
        }

        /**
         * Sets the list of update details.
         *
         * @param updateDetails the list of update details to set
         */
        public void setUpdateDetails(List<UpdateChunkDetail> updateDetails) {
            this.updateDetails = updateDetails;
        }
    }

    /**
     * Details of an updated chunk.
     * <p>
     * This class contains information about a chunk that was updated,
     * including document ID, OSS key, chunk ID, update status, and failure details.
     * </p>
     */
    public static class UpdateChunkDetail {
        /**
         * The document ID.
         */
        private String docId;
        
        /**
         * The OSS key for the document.
         */
        private String ossKey;
        
        /**
         * The chunk ID.
         */
        private Integer chunkId;
        
        /**
         * The status of the chunk update.
         */
        private String updateStatus;
        
        /**
         * The details of the failure if the update failed.
         */
        private String failureReason;

        /**
         * Default constructor.
         */
        public UpdateChunkDetail() {
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
         * Gets the chunk ID.
         *
         * @return the chunk ID
         */
        public Integer getChunkId() {
            return chunkId;
        }

        /**
         * Sets the chunk ID.
         *
         * @param chunkId the chunk ID to set
         */
        public void setChunkId(Integer chunkId) {
            this.chunkId = chunkId;
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
         * Gets the failure details.
         *
         * @return the failure details, or null if successful
         */
        public String getFailureReason() {
            return failureReason;
        }

        /**
         * Sets the failure details.
         *
         * @param failureReason the failure details to set
         */
        public void setFailureReason(String failureReason) {
            this.failureReason = failureReason;
        }
    }
}
