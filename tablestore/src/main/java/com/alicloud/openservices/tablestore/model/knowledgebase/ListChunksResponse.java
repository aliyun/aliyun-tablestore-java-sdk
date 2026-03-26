package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;

/**
 * Response for listing chunks in a knowledge base.
 * <p>
 * This class encapsulates the response returned after listing document chunks,
 * including pagination support and chunk details.
 * </p>
 */
public class ListChunksResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the list of chunks and pagination token.
     */
    private ListChunksData data;

    /**
     * Default constructor.
     */
    public ListChunksResponse() {
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
     * Gets the data containing chunks and pagination token.
     *
     * @return the list chunks data
     */
    public ListChunksData getData() {
        return data;
    }

    /**
     * Sets the data containing chunks and pagination token.
     *
     * @param data the list chunks data to set
     */
    public void setData(ListChunksData data) {
        this.data = data;
    }

    /**
     * Data container for listing chunks.
     * <p>
     * This class contains the list of chunk details and a pagination token
     * for retrieving the next page of results.
     * </p>
     */
    public static class ListChunksData {
        /**
         * The list of chunk details.
         */
        private List<ChunkDetail> chunkDetails;
        
        /**
         * The pagination token for retrieving the next page of results.
         * Null if there are no more results.
         */
        private String nextToken;

        /**
         * Default constructor.
         */
        public ListChunksData() {
        }

        /**
         * Gets the list of chunk details.
         *
         * @return the list of chunk details
         */
        public List<ChunkDetail> getChunkDetails() {
            return chunkDetails;
        }

        /**
         * Sets the list of chunk details.
         *
         * @param chunkDetails the list of chunk details to set
         */
        public void setChunkDetails(List<ChunkDetail> chunkDetails) {
            this.chunkDetails = chunkDetails;
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

    /**
     * Details of a document chunk.
     * <p>
     * This class contains comprehensive information about a document chunk,
     * including subspace, document ID, OSS key, chunk ID, content, title,
     * chunk type, status, and timestamps.
     * </p>
     */
    public static class ChunkDetail {
        /**
         * The subspace where the chunk is located.
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
         * The chunk ID within the document.
         */
        private Integer chunkId;
        
        /**
         * The content of the chunk.
         */
        private String content;
        
        /**
         * The title of the chunk.
         */
        private String title;
        
        /**
         * The type of the chunk.
         */
        private String chunkType;
        
        /**
         * The status of the chunk.
         */
        private String status;
        
        /**
         * The timestamp when the chunk was created (Unix timestamp in milliseconds).
         */
        private Long createdAt;
        
        /**
         * The timestamp when the chunk was last updated (Unix timestamp in milliseconds).
         */
        private Long updatedAt;

        /**
         * Default constructor.
         */
        public ChunkDetail() {
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
         * Gets the content of the chunk.
         *
         * @return the chunk content
         */
        public String getContent() {
            return content;
        }

        /**
         * Sets the content of the chunk.
         *
         * @param content the chunk content to set
         */
        public void setContent(String content) {
            this.content = content;
        }

        /**
         * Gets the title of the chunk.
         *
         * @return the chunk title
         */
        public String getTitle() {
            return title;
        }

        /**
         * Sets the title of the chunk.
         *
         * @param title the chunk title to set
         */
        public void setTitle(String title) {
            this.title = title;
        }

        /**
         * Gets the chunk type.
         *
         * @return the chunk type
         */
        public String getChunkType() {
            return chunkType;
        }

        /**
         * Sets the chunk type.
         *
         * @param chunkType the chunk type to set
         */
        public void setChunkType(String chunkType) {
            this.chunkType = chunkType;
        }

        /**
         * Gets the chunk status.
         *
         * @return the chunk status
         */
        public String getStatus() {
            return status;
        }

        /**
         * Sets the chunk status.
         *
         * @param status the chunk status to set
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Gets the creation timestamp.
         *
         * @return the creation timestamp in milliseconds
         */
        public Long getCreatedAt() {
            return createdAt;
        }

        /**
         * Sets the creation timestamp.
         *
         * @param createdAt the creation timestamp in milliseconds
         */
        public void setCreatedAt(Long createdAt) {
            this.createdAt = createdAt;
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
