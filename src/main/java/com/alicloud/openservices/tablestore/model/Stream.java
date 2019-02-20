package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class Stream implements Jsonizable {

    /**
     * Stream的Id
     */
    private String streamId;

    /**
     * 所属的表的名称
     */
    private String tableName;

    /**
     * Stream的创建时间，单位微秒
     */
    private long creationTime;

    /**
     * 设置streamId
     * @param streamId
     */
    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * 获取streamId
     * @return
     */
    public String getStreamId() {
        return streamId;
    }

    /**
     * 设置所属表名
     * @param tableName
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 获取所属表名
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置Stream的创建时间，单位微秒
     * @param creationTime
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * 获取Stream的创建时间，单位微秒
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
