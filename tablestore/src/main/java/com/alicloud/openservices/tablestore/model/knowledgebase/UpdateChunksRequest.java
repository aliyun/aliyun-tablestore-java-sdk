package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * Request class for updating chunks in a knowledge base.
 * <p>
 * This class represents a request to update one or more document chunks
 * in a knowledge base. Each chunk can be identified by its chunk ID,
 * and you can update its content, title, status, and other properties.
 * </p>
 */
public class UpdateChunksRequest implements Request {

    /**
     * Custom JSON string. If set, this will be used instead of auto-serialization.
     */
    private transient String jsonStr;

    /**
     * Name of the knowledge base.
     */
    private String knowledgeBaseName;

    /**
     * Subspace within the knowledge base.
     */
    private String subspace;

    /**
     * List of chunks to update.
     */
    private List<UpdateChunkItem> chunks;

    /**
     * Constructs a new UpdateChunksRequest.
     */
    public UpdateChunksRequest() {
    }

    /**
     * Constructs a new UpdateChunksRequest with the specified knowledge base name.
     *
     * @param knowledgeBaseName the name of the knowledge base
     */
    public UpdateChunksRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    /**
     * Gets the knowledge base name.
     *
     * @return the knowledge base name
     */
    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    /**
     * Sets the knowledge base name.
     *
     * @param knowledgeBaseName the knowledge base name
     */
    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
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
     * @param subspace the subspace
     */
    public void setSubspace(String subspace) {
        this.subspace = subspace;
    }

    /**
     * Gets the list of chunks to update.
     *
     * @return the list of chunk items
     */
    public List<UpdateChunkItem> getChunks() {
        return chunks;
    }

    /**
     * Sets the list of chunks to update.
     *
     * @param chunks the list of chunk items
     */
    public void setChunks(List<UpdateChunkItem> chunks) {
        this.chunks = chunks;
    }

    /**
     * Gets the custom JSON string.
     *
     * @return the custom JSON string
     */
    public String getJsonStr() {
        return jsonStr;
    }

    /**
     * Sets the custom JSON string. If set, this will be used instead of auto-serialization.
     *
     * @param jsonStr the custom JSON string
     */
    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    /**
     * Gets the operation name for this request.
     *
     * @return the operation name
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_CHUNKS;
    }

    /**
     * Converts this request to JSON string.
     * <p>
     * If a custom JSON string is set via {@link #setJsonStr(String)}, it will be returned.
     * Otherwise, this object will be serialized using Gson.
     * </p>
     * <p>
     * When {@link UpdateChunkItem#setTitle(String)} or {@link UpdateChunkItem#setContent(String)}
     * is called with {@code null}, the resulting JSON will contain the corresponding field as
     * {@code null} to signal deletion on the server. When these setters are not called at all,
     * the fields will be absent from the JSON, leaving the server-side values unchanged.
     * </p>
     *
     * @return the JSON representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        if (!hasExplicitlyNullChunkFields()) {
            return GsonUtils.toJson(this);
        }
        JsonObject root = new JsonObject();
        if (knowledgeBaseName != null) {
            root.addProperty("knowledgeBaseName", knowledgeBaseName);
        }
        if (subspace != null) {
            root.addProperty("subspace", subspace);
        }
        if (chunks != null) {
            JsonArray chunksArray = new JsonArray();
            for (UpdateChunkItem chunk : chunks) {
                chunksArray.add(chunk.toJsonObject());
            }
            root.add("chunks", chunksArray);
        }
        return root.toString();
    }

    private boolean hasExplicitlyNullChunkFields() {
        if (chunks == null) {
            return false;
        }
        for (UpdateChunkItem chunk : chunks) {
            if (chunk.hasExplicitlyNullFields()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Represents a chunk item to be updated.
     */
    public static class UpdateChunkItem {
        /**
         * Document ID.
         */
        private String docId;

        /**
         * OSS key.
         */
        private String ossKey;

        /**
         * Chunk ID.
         */
        private Integer chunkId;

        /**
         * Chunk title.
         */
        private String title;

        /**
         * Tracks whether {@link #setTitle(String)} has been explicitly called.
         */
        private transient boolean titleExplicitlySet = false;

        /**
         * Chunk content.
         */
        private String content;

        /**
         * Tracks whether {@link #setContent(String)} has been explicitly called.
         */
        private transient boolean contentExplicitlySet = false;

        /**
         * Chunk status.
         */
        private String status;

        /**
         * Constructs a new UpdateChunkItem.
         */
        public UpdateChunkItem() {
        }

        /**
         * Constructs a new UpdateChunkItem with the specified chunk ID.
         *
         * @param chunkId the chunk ID
         */
        public UpdateChunkItem(Integer chunkId) {
            this.chunkId = chunkId;
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
         * @param docId the document ID
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
         * @param ossKey the OSS key
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
         * @param chunkId the chunk ID
         */
        public void setChunkId(Integer chunkId) {
            this.chunkId = chunkId;
        }

        /**
         * Gets the chunk title.
         *
         * @return the chunk title
         */
        public String getTitle() {
            return title;
        }

        /**
         * Sets the chunk title.
         * <p>
         * Setting title to a non-null value will update the title on the server.
         * Setting title to {@code null} explicitly will delete the title on the server.
         * Not calling this method at all will leave the title unchanged.
         * </p>
         *
         * @param title the chunk title, or {@code null} to delete the title
         */
        public void setTitle(String title) {
            this.title = title;
            this.titleExplicitlySet = true;
        }

        /**
         * Gets the chunk content.
         *
         * @return the chunk content
         */
        public String getContent() {
            return content;
        }

        /**
         * Sets the chunk content.
         * <p>
         * Setting content to a non-null value will update the content on the server.
         * Setting content to {@code null} explicitly will delete the content on the server.
         * Not calling this method at all will leave the content unchanged.
         * </p>
         *
         * @param content the chunk content, or {@code null} to delete the content
         */
        public void setContent(String content) {
            this.content = content;
            this.contentExplicitlySet = true;
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
         * @param status the chunk status
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Returns whether this chunk item has any fields that were explicitly set to null.
         */
        boolean hasExplicitlyNullFields() {
            return (titleExplicitlySet && title == null) || (contentExplicitlySet && content == null);
        }

        /**
         * Converts this chunk item to a JsonObject, preserving explicitly null fields.
         */
        JsonObject toJsonObject() {
            JsonObject obj = new JsonObject();
            if (docId != null) {
                obj.addProperty("docId", docId);
            }
            if (ossKey != null) {
                obj.addProperty("ossKey", ossKey);
            }
            if (chunkId != null) {
                obj.addProperty("chunkId", chunkId);
            }
            if (title != null) {
                obj.addProperty("title", title);
            } else if (titleExplicitlySet) {
                obj.add("title", JsonNull.INSTANCE);
            }
            if (content != null) {
                obj.addProperty("content", content);
            } else if (contentExplicitlySet) {
                obj.add("content", JsonNull.INSTANCE);
            }
            if (status != null) {
                obj.addProperty("status", status);
            }
            return obj;
        }
    }
}
