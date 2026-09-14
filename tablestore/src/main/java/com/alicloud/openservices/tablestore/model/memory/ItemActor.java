package com.alicloud.openservices.tablestore.model.memory;

public class ItemActor {
    private String actorType;
    private String actorId;

    public ItemActor() {
    }

    public ItemActor(String actorType, String actorId) {
        this.actorType = actorType;
        this.actorId = actorId;
    }

    public String getActorType() {
        return actorType;
    }

    public void setActorType(String actorType) {
        this.actorType = actorType;
    }

    public String getActorId() {
        return actorId;
    }

    public void setActorId(String actorId) {
        this.actorId = actorId;
    }
}
