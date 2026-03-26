package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.List;
import java.util.Map;

/**
 * Response for retrieving documents from a knowledge base.
 * <p>
 * This class encapsulates the response returned after a retrieval operation,
 * including the retrieved documents and their relevance scores.
 * </p>
 */
public class RetrieveResponse extends Response {
    /**
     * The response code indicating the status of the operation.
     */
    private String code;
    
    /**
     * The response message providing additional information about the operation.
     */
    private String message;
    
    /**
     * The data containing the retrieval results.
     */
    private RetrieveData data;

    /**
     * Default constructor.
     */
    public RetrieveResponse() {
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
     * Gets the data containing retrieval results.
     *
     * @return the retrieve data
     */
    public RetrieveData getData() {
        return data;
    }

    /**
     * Sets the data containing retrieval results.
     *
     * @param data the retrieve data to set
     */
    public void setData(RetrieveData data) {
        this.data = data;
    }

    /**
     * Data container for retrieval results.
     * <p>
     * This class contains the list of retrieval results.
     * </p>
     */
    public static class RetrieveData {
        /**
         * The list of retrieval results.
         */
        private List<RetrievalResult> retrievalResults;

        /**
         * Default constructor.
         */
        public RetrieveData() {
        }

        /**
         * Gets the list of retrieval results.
         *
         * @return the list of retrieval results
         */
        public List<RetrievalResult> getRetrievalResults() {
            return retrievalResults;
        }

        /**
         * Sets the list of retrieval results.
         *
         * @param retrievalResults the list of retrieval results to set
         */
        public void setRetrievalResults(List<RetrievalResult> retrievalResults) {
            this.retrievalResults = retrievalResults;
        }
    }

    /**
     * A single retrieval result.
     * <p>
     * This class contains information about a retrieved document chunk,
     * including document ID, chunk ID, score, content, and metadata.
     * </p>
     */
    public static class RetrievalResult {
        /**
         * The document ID.
         */
        private String docId;
        
        /**
         * The chunk ID within the document.
         */
        private Integer chunkId;
        
        /**
         * The OSS key for the document.
         */
        private String ossKey;
        
        /**
         * The relevance score of this result.
         */
        private Double score;
        
        /**
         * The content of the chunk.
         */
        private String content;
        
        /**
         * The subspace where the chunk is located.
         */
        private String subspace;
        
        /**
         * The metadata associated with the chunk.
         */
        private Map<String, Object> metadata;

        /**
         * Default constructor.
         */
        public RetrievalResult() {
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
         * Gets the relevance score.
         *
         * @return the relevance score
         */
        public Double getScore() {
            return score;
        }

        /**
         * Sets the relevance score.
         *
         * @param score the relevance score to set
         */
        public void setScore(Double score) {
            this.score = score;
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
}
