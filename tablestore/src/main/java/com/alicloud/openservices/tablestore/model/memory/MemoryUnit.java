package com.alicloud.openservices.tablestore.model.memory;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import java.util.Map;

public class MemoryUnit {
    private String id;
    @SerializedName("conversation_key")
    private String conversationKey;
    private Scope scope;
    @SerializedName("memcell_id")
    private String memcellId;
    @SerializedName("unit_type")
    private String unitType;
    private String text;
    @SerializedName("search_text")
    private String searchText;
    private List<String> entities;
    @SerializedName("time_anchor")
    private Map<String, String> timeAnchor;
    @SerializedName("source_turn_ids")
    private List<String> sourceTurnIds;
    private String speaker;
    private String topic;
    @SerializedName("type_label")
    private String typeLabel;
    @SerializedName("date_bucket")
    private String dateBucket;
    private Map<String, String> metadata;
    @SerializedName("metadata_json")
    private String metadataJson;
    @SerializedName("metadata_flat")
    private String metadataFlat;
    private Boolean deleted;
    @SerializedName("created_at")
    private String createdAt;
    private Double salience;
    private Integer version;
    @SerializedName("superseded_by")
    private String supersededBy;
    @SerializedName("supersedes_id")
    private String supersedesId;

    public MemoryUnit() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getConversationKey() {
        return conversationKey;
    }

    public void setConversationKey(String conversationKey) {
        this.conversationKey = conversationKey;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getMemcellId() {
        return memcellId;
    }

    public void setMemcellId(String memcellId) {
        this.memcellId = memcellId;
    }

    public String getUnitType() {
        return unitType;
    }

    public void setUnitType(String unitType) {
        this.unitType = unitType;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getSearchText() {
        return searchText;
    }

    public void setSearchText(String searchText) {
        this.searchText = searchText;
    }

    public List<String> getEntities() {
        return entities;
    }

    public void setEntities(List<String> entities) {
        this.entities = entities;
    }

    public Map<String, String> getTimeAnchor() {
        return timeAnchor;
    }

    public void setTimeAnchor(Map<String, String> timeAnchor) {
        this.timeAnchor = timeAnchor;
    }

    public List<String> getSourceTurnIds() {
        return sourceTurnIds;
    }

    public void setSourceTurnIds(List<String> sourceTurnIds) {
        this.sourceTurnIds = sourceTurnIds;
    }

    public String getSpeaker() {
        return speaker;
    }

    public void setSpeaker(String speaker) {
        this.speaker = speaker;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTypeLabel() {
        return typeLabel;
    }

    public void setTypeLabel(String typeLabel) {
        this.typeLabel = typeLabel;
    }

    public String getDateBucket() {
        return dateBucket;
    }

    public void setDateBucket(String dateBucket) {
        this.dateBucket = dateBucket;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String getMetadataJson() {
        return metadataJson;
    }

    public void setMetadataJson(String metadataJson) {
        this.metadataJson = metadataJson;
    }

    public String getMetadataFlat() {
        return metadataFlat;
    }

    public void setMetadataFlat(String metadataFlat) {
        this.metadataFlat = metadataFlat;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public Double getSalience() {
        return salience;
    }

    public void setSalience(Double salience) {
        this.salience = salience;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getSupersededBy() {
        return supersededBy;
    }

    public void setSupersededBy(String supersededBy) {
        this.supersededBy = supersededBy;
    }

    public String getSupersedesId() {
        return supersedesId;
    }

    public void setSupersedesId(String supersedesId) {
        this.supersedesId = supersedesId;
    }
}
