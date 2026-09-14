package com.alicloud.openservices.tablestore.model.memory;

public class ItemVersion {
    private String versionId;
    private String itemId;
    private Scope scope;
    private String operation;
    private String path;
    private String content;
    private String contentSha256;
    private Long contentSizeBytes;
    private ItemActor createdBy;
    private String createdAt;
    private Boolean redacted;
    private String redactedAt;
    private ItemActor redactedBy;

    public ItemVersion() {
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Scope getScope() {
        return scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContentSha256() {
        return contentSha256;
    }

    public void setContentSha256(String contentSha256) {
        this.contentSha256 = contentSha256;
    }

    public Long getContentSizeBytes() {
        return contentSizeBytes;
    }

    public void setContentSizeBytes(Long contentSizeBytes) {
        this.contentSizeBytes = contentSizeBytes;
    }

    public ItemActor getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(ItemActor createdBy) {
        this.createdBy = createdBy;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public Boolean getRedacted() {
        return redacted;
    }

    public void setRedacted(Boolean redacted) {
        this.redacted = redacted;
    }

    public String getRedactedAt() {
        return redactedAt;
    }

    public void setRedactedAt(String redactedAt) {
        this.redactedAt = redactedAt;
    }

    public ItemActor getRedactedBy() {
        return redactedBy;
    }

    public void setRedactedBy(ItemActor redactedBy) {
        this.redactedBy = redactedBy;
    }
}
