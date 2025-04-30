package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.*;

public class StreamDetails implements Jsonizable {

    /**
     * Whether to enable Stream
     */
    private boolean enableStream;

    /**
     * The Id of the Stream
     * Valid when the Stream is enabled
     */
    private String streamId;

    /**
     * The expiration time of the Stream, in hours.
     * Valid when the Stream is enabled.
     */
    private int expirationTime;

    /**
     * The time when the Stream was last enabled, in microseconds.
     */
    private long lastEnableTime;

    /**
     * Set the original column list in the Stream data
     */
    private Set<String> originColumnsToGet = new HashSet<String>();

    public StreamDetails() {

    }

    public StreamDetails(boolean enableStream, String streamId, int expirationTime, int lastEnableTime) {
        setEnableStream(enableStream);
        setStreamId(streamId);
        setExpirationTime(expirationTime);
        setLastEnableTime(lastEnableTime);
    }

    /**
     * Get whether Stream is enabled
     * @return Whether Stream is enabled
     */
    public boolean isEnableStream() {
        return enableStream;
    }

    public void setEnableStream(boolean enableStream) {
        this.enableStream = enableStream;
    }

    /**
     * Get the Id of the Stream
     * @return The Id of the Stream
     */
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    /**
     * Get the expiration time of the Stream, in hours.
     * @return The expiration time of the Stream.
     */
    public int getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * Get the time of the last Stream start, in microseconds.
     * @return The time of the last Stream start.
     */
    public long getLastEnableTime() {
        return lastEnableTime;
    }

    public void setLastEnableTime(long lastEnableTime) {
        this.lastEnableTime = lastEnableTime;
    }

    /**
     * Returns the list of names of the original columns to be read (read-only).
     *
     * @return The list of names of the original columns (read-only).
     */
    public Set<String> getOriginColumnsToGet() {
        return Collections.unmodifiableSet(originColumnsToGet);
    }

    /**
     * Add the raw column to be read.
     *
     * @param OriginColumnName The name of the raw column to be returned.
     */
    public void addOriginColumnsToGet(String OriginColumnName) {
        Preconditions.checkArgument(OriginColumnName != null && !OriginColumnName.isEmpty(), "OriginColumn's name should not be null or empty.");
        this.originColumnsToGet.add(OriginColumnName);
    }

    /**
     * Add the raw columns to be read.
     *
     * @param originColumnNames The names of the raw columns to be returned.
     */
    public void addOriginColumnsToGet(String[] originColumnNames) {
        Preconditions.checkNotNull(originColumnNames, "columnNames should not be null.");
        for (int i = 0; i < originColumnNames.length; ++i) {
            addOriginColumnsToGet(originColumnNames[i]);
        }
    }

    /**
     * Add the raw columns to be read.
     *
     * @param originColumnsToGet
     */
    public void addOriginColumnsToGet(Collection<String> originColumnsToGet) {
        this.originColumnsToGet.addAll(originColumnsToGet);
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
        sb.append("\"OriginColumnToGet\": ");
        sb.append(originColumnsToGet);
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
        sb.append(", OriginColumnToGet: ");
        sb.append(originColumnsToGet);
        return sb.toString();
    }
}
