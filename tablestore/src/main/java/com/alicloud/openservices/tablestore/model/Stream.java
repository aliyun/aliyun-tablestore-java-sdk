package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class Stream implements Jsonizable {

    /**
     * The Id of the Stream
     */
    private String streamId;

    /**
     * The name of the table it belongs to
     */
    private String tableName;

    /**
     * The creation time of the Stream, in microseconds.
     */
    private long creationTime;

    /**
     * Set streamId
     * @param streamId
     */
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * Get the streamId
     * @return
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * Set the table name to which it belongs
     * @param tableName
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the name of the table it belongs to
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set the creation time of the Stream, in microseconds.
     * @param creationTime
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Get the creation time of the Stream, in microseconds.
     * @return
     */
    public long getCreationTime() {
        return creationTime;
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
        sb.append("\"StreamId\": ");
        sb.append("\"");
        sb.append(streamId);
        sb.append("\", ");
        sb.append(newline);
        sb.append("\"TableName\": ");
        sb.append("\"");
        sb.append(tableName);
        sb.append("\", ");
        sb.append(newline);
        sb.append("\"CreationTime\": ");
        sb.append(creationTime);
        sb.append(newline);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StreamId: ");
        sb.append(streamId);
        sb.append(", TableName: ");
        sb.append(tableName);
        sb.append(", CreationTime: ");
        sb.append(creationTime);
        return sb.toString();
    }
}
