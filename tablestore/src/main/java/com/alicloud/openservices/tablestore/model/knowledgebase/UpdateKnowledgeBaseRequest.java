package com.alicloud.openservices.tablestore.model.knowledgebase;

import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.List;

public class UpdateKnowledgeBaseRequest implements Request {

    private transient String jsonStr;
    private String knowledgeBaseName;
    private String description;
    private transient boolean descriptionExplicitlySet = false;
    private List<String> tags;
    private transient boolean tagsExplicitlySet = false;
    private RetrievalConfiguration retrievalConfiguration;

    public UpdateKnowledgeBaseRequest() {
    }

    public UpdateKnowledgeBaseRequest(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    public String getKnowledgeBaseName() {
        return knowledgeBaseName;
    }

    public void setKnowledgeBaseName(String knowledgeBaseName) {
        this.knowledgeBaseName = knowledgeBaseName;
    }

    public String getDescription() {
        return description;
    }

    /**
     * Sets the description.
     * <p>
     * Setting description to a non-null value will update it on the server.
     * Setting description to {@code null} explicitly will delete it on the server.
     * Not calling this method at all will leave the description unchanged.
     * </p>
     *
     * @param description the description, or {@code null} to delete
     */
    public void setDescription(String description) {
        this.description = description;
        this.descriptionExplicitlySet = true;
    }

    public List<String> getTags() {
        return tags;
    }

    /**
     * Sets the tags.
     * <p>
     * Setting tags to a non-null list will update them on the server.
     * Setting tags to {@code null} explicitly will delete them on the server.
     * Not calling this method at all will leave the tags unchanged.
     * </p>
     *
     * @param tags the tags list, or {@code null} to delete
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
        this.tagsExplicitlySet = true;
    }

    public RetrievalConfiguration getRetrievalConfiguration() {
        return retrievalConfiguration;
    }

    public void setRetrievalConfiguration(RetrievalConfiguration retrievalConfiguration) {
        this.retrievalConfiguration = retrievalConfiguration;
    }

    public String getJsonStr() {
        return jsonStr;
    }

    public void setJsonStr(String jsonStr) {
        this.jsonStr = jsonStr;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_KNOWLEDGE_BASE;
    }

    /**
     * Converts this request to JSON string.
     * <p>
     * When {@link #setDescription(String)} or {@link #setTags(List)} is called with {@code null},
     * the resulting JSON will contain the corresponding field as {@code null} to signal deletion
     * on the server. When these setters are not called at all, the fields will be absent from
     * the JSON, leaving the server-side values unchanged.
     * </p>
     *
     * @return the JSON representation of this request
     */
    public String toJson() {
        if (jsonStr != null) {
            return jsonStr;
        }
        boolean hasExplicitNull = (descriptionExplicitlySet && description == null)
                || (tagsExplicitlySet && tags == null);
        if (!hasExplicitNull) {
            return GsonUtils.toJson(this);
        }
        JsonObject jsonObject = new JsonObject();
        if (knowledgeBaseName != null) {
            jsonObject.addProperty("knowledgeBaseName", knowledgeBaseName);
        }
        if (description != null) {
            jsonObject.addProperty("description", description);
        } else if (descriptionExplicitlySet) {
            jsonObject.add("description", JsonNull.INSTANCE);
        }
        if (tags != null) {
            jsonObject.add("tags", GsonUtils.getGson().toJsonTree(tags));
        } else if (tagsExplicitlySet) {
            jsonObject.add("tags", JsonNull.INSTANCE);
        }
        if (retrievalConfiguration != null) {
            jsonObject.add("retrievalConfiguration", GsonUtils.getGson().toJsonTree(retrievalConfiguration));
        }
        return jsonObject.toString();
    }
}
