package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class StreamShard implements Jsonizable {
    /**
     * Id of the Shard
     */
    private String shardId;

    /**
     * The Id of the parent Shard
     */
    private String parentId;

    /**
     * The Id of the parent or peer Shard
     */
    private String parentSiblingId;

    public StreamShard(String shardId) {
        setShardId(shardId);
    }

    /**
     * Get ShardId
     * @return shardId
     */
    public String getShardId() {
        return shardId;
    }

    public void setShardId(String shardId) {
        this.shardId = shardId;
    }

    /**
     * Get ParentId
     * @return parentId
     */
    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    /**
     * Get ParentSiblingId
     * @return parentSiblingId
     */
    public String getParentSiblingId() {
        return parentSiblingId;
    }

    public void setParentSiblingId(String parentSiblingId) {
        this.parentSiblingId = parentSiblingId;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"ShardId\": ");
        sb.append("\"");
        sb.append(shardId);
        sb.append("\", ");
        sb.append(newline);
        sb.append("\"ParentId\": ");
        sb.append("\"");
        sb.append(parentId);
        sb.append("\", ");
        sb.append(newline);
        sb.append("\"ParentSiblingId\": ");
        sb.append("\"");
        sb.append(parentSiblingId);
        sb.append("\"");
        sb.append(newline);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ShardId: ");
        sb.append(shardId);
        sb.append(", ParentId: ");
        sb.append(parentId);
        sb.append(", ParentSiblingId: ");
        sb.append(parentSiblingId);
        return sb.toString();
    }
}
