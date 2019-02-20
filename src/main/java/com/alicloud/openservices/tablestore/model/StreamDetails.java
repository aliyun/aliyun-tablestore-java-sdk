package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class StreamDetails implements Jsonizable {

    /**
     * 是否开启Stream
     */
    private boolean enableStream;

    /**
     * Stream的Id
     * 开启Stream时有效
     */
    private String streamId;

    /**
     * Stream的过期时间，单位小时
     * 开启Stream时有效
     */
    private int expirationTime;

    /**
     * 上次开启Stream的时间，单位微秒
     */
    private long lastEnableTime;

    public StreamDetails() {

    }

    public StreamDetails(boolean enableStream, String streamId, int expirationTime, int lastEnableTime) {
        setEnableStream(enableStream);
        setStreamId(streamId);
        setExpirationTime(expirationTime);
        setLastEnableTime(lastEnableTime);
    }

    /**
     * 获取是否开启Stream
     * @return 是否开启Stream
     */
    public boolean isEnableStream() {
        return enableStream;
    }

    public void setEnableStream(boolean enableStream) {
        this.enableStream = enableStream;
    }

    /**
     * 获取Stream的Id
     * @return Stream的Id
     */
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * 获取Stream的过期时间，单位小时
     * @return Stream的过期时间
     */
    public int getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * 获取上次开启Stream的时间，单位微秒
     * @return 上次开启Stream的时间
     */
    public long getLastEnableTime() {
        return lastEnableTime;
    }

    public void setLastEnableTime(long lastEnableTime) {
        this.lastEnableTime = lastEnableTime;
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
        sb.append("\"EnableStream\": ");
        sb.append(enableStream);
        sb.append(",");
        sb.append(newline);
        sb.append("\"StreamId\": ");
        sb.append("\"" + streamId + "\"");
        sb.append(",");
        sb.append(newline);
        sb.append("\"ExpirationTime\": ");
        sb.append(expirationTime);
        sb.append(",");
        sb.append(newline);
        sb.append("\"LastEnableTime\": ");
        sb.append(lastEnableTime);
        sb.append(newline);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EnableStream: ");
        sb.append(enableStream);
        sb.append(", StreamId: ");
        sb.append(streamId);
        sb.append(", ExpirationTime: ");
        sb.append(expirationTime);
        sb.append(", LastEnableTime: ");
        sb.append(lastEnableTime);
        return sb.toString();
    }
}
