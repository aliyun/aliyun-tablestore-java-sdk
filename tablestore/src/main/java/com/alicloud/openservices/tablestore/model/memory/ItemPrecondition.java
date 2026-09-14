package com.alicloud.openservices.tablestore.model.memory;

public class ItemPrecondition {
    private String expectedVersionId;

    public ItemPrecondition() {
    }

    public ItemPrecondition(String expectedVersionId) {
        this.expectedVersionId = expectedVersionId;
    }

    public String getExpectedVersionId() {
        return expectedVersionId;
    }

    public void setExpectedVersionId(String expectedVersionId) {
        this.expectedVersionId = expectedVersionId;
    }
}
