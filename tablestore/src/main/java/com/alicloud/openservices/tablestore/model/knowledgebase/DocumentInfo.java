package com.alicloud.openservices.tablestore.model.knowledgebase;

import java.io.Serializable;
import java.util.Map;

/**
 * Information about a document in a knowledge base.
 * <p>
 * This class contains comprehensive information about a document,
 * including subspace, document ID, OSS key, timestamps, status,
 * chunk count, and metadata.
 * </p>
 */
public class DocumentInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The subspace where the document is located.
     */
    private String subspace;
    
    /**
     * The document ID.
     */
    private String docId;
    
    /**
     * The OSS key for the document.
     */
    private String ossKey;
    
    /**
     * The timestamp when the document was created (Unix timestamp in milliseconds).
     */
    private long createdAt;
    
    /**
     * The timestamp when the document was last updated (Unix timestamp in milliseconds).
     */
    private long updatedAt;
    
    /**
     * The entity tag for the document.
     */
    private String eTag;
    
    /**
     * The status of the document.
     */
    private DocumentStatus status;
    
    /**
     * The details of the failure if the document processing failed.
     */
    private String failedDetails;
    
    /**
     * The number of chunks in the document.
     */
    private int chunkNum;
    
    /**
     * The metadata associated with the document.
     */
    private Map<String, Object> metadata;

    /**
     * Default constructor.
     */
    public DocumentInfo() {
    }

    /**
     * Gets the subspace.
     *
     * @return the subspace
     */
    public String getSubspace() {
        return subspace;
    }

    /**
     * Sets the subspace.
     *
     * @param subspace the subspace to set
     */
    public void setSubspace(String subspace) {
        this.subspace = subspace;
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
     * Gets the creation timestamp.
     *
     * @return the creation timestamp in milliseconds
     */
    public long getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the creation timestamp.
     *
     * @param createdAt the creation timestamp in milliseconds
     */
    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Gets the last update timestamp.
     *
     * @return the last update timestamp in milliseconds
     */
    public long getUpdatedAt() {
        return updatedAt;
    }

    /**
     * Sets the last update timestamp.
     *
     * @param updatedAt the last update timestamp in milliseconds
     */
    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * Gets the entity tag.
     *
     * @return the entity tag
     */
    public String getETag() {
        return eTag;
    }

    /**
     * Sets the entity tag.
     *
     * @param eTag the entity tag to set
     */
    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    /**
     * Gets the document status.
     *
     * @return the document status
     */
    public DocumentStatus getStatus() {
        return status;
    }

    /**
     * Sets the document status.
     *
     * @param status the document status to set
     */
    public void setStatus(DocumentStatus status) {
        this.status = status;
    }

    /**
     * Gets the failure details.
     *
     * @return the failure details, or null if successful
     */
    public String getFailedDetails() {
        return failedDetails;
    }

    /**
     * Sets the failure details.
     *
     * @param failedDetails the failure details to set
     */
    public void setFailedDetails(String failedDetails) {
        this.failedDetails = failedDetails;
    }

    /**
     * Gets the number of chunks.
     *
     * @return the chunk count
     */
    public int getChunkNum() {
        return chunkNum;
    }

    /**
     * Sets the number of chunks.
     *
     * @param chunkNum the chunk count to set
     */
    public void setChunkNum(int chunkNum) {
        this.chunkNum = chunkNum;
    }

    /**
     * Gets the metadata.
     *
     * @return the metadata map
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Sets the metadata.
     *
     * @param metadata the metadata map to set
     */
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
