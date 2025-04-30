package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StreamSpecification implements Jsonizable {
    /**
     * Whether to enable Stream
     */
    private boolean enableStream = false;

    /**
     * When the Stream is enabled, this parameter is used to set the data expiration time, in hours.
     */
    private OptionalValue<Integer> expirationTime = new OptionalValue<Integer>("ExpirationTime");

    /**
     * Set the original column list in the Stream data
     */
    private Set<String> originColumnsToGet = new HashSet<String>();


    /**
     * Construct a StreamSpecification object.
     * Note: The enableStream parameter must be set to false. The reason is that when enableStream is true, expirationTime must be specified.
     *       If you need to set Stream to be enabled, please use the other constructor.
     * @param enableStream Must be false, indicating that Stream is disabled.
     */
    public StreamSpecification(boolean enableStream) {
        if (enableStream) {
            throw new ClientException("Expiration time is required when enableStream is true.");
        }
        setEnableStream(enableStream);
    }

    /**
     * Construct a StreamSpecification object.
     * Note: The enableStream parameter must be true because when enableStream is false, expirationTime cannot be specified.
     *       If you need to set the Stream to be off, please use the other constructor.
     * @param enableStream Must be true, indicating that the Stream is enabled.
     * @param expirationTime In hours, must be greater than 0.
     */
    public StreamSpecification(boolean enableStream, int expirationTime) {
        if (!enableStream) {
            throw new ClientException("Expiration time cannot be set when enableStream is false.");
        }
        setEnableStream(enableStream);
        setExpirationTime(expirationTime);
    }

    /**
     * Returns whether Stream is enabled
     *
     * @return
     */
    public boolean isEnableStream() {
        return enableStream;
    }

    /**
     * Set whether to enable Stream
     *
     * @param enableStream
     */
    public void setEnableStream(boolean enableStream) {
        this.enableStream = enableStream;
    }

    /**
     * Get the expirationTime parameter, in hours.
     *
     * @return expirationTime, if -1 is returned, it means that this value has not been set.
     */
    public int getExpirationTime() {
        if (expirationTime.isValueSet()) {
            return expirationTime.getValue();
        } else {
            return -1;
        }
    }

    /**
     * Set the expirationTime parameter, in hours.
     *
     * @param expirationTime
     */
    public void setExpirationTime(int expirationTime) {
        Preconditions.checkArgument(expirationTime > 0, "The expiration time must be greater than 0.");
        this.expirationTime.setValue(expirationTime);
    }

    /**
     * Returns the list of names of the original columns to be read (read-only).
     *
     * @return A list of names of the original columns (read-only).
     */
    public Set<String> getOriginColumnsToGet() {
        return Collections.unmodifiableSet(originColumnsToGet);
    }

    /**
     * Add the raw column to be read.
     *
     * @param originColumnName The name of the raw column to be returned.
     */
    public void addOriginColumnsToGet(String originColumnName) {
        Preconditions.checkArgument(originColumnName != null && !originColumnName.isEmpty(), "OriginColumn's name should not be null or empty.");
        this.originColumnsToGet.add(originColumnName);
    }

    /**
     * Add the original columns to be read.
     *
     * @param originColumnNames The names of the original columns to be returned.
     */
    public void addOriginColumnsToGet(String[] originColumnNames) {
        Preconditions.checkNotNull(originColumnNames, "originColumnNames should not be null.");
        for (int i = 0; i < originColumnNames.length; ++i) {
            addOriginColumnsToGet(originColumnNames[i]);
        }
    }

    /**
     * Add the raw columns to read.
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
        if (expirationTime.isValueSet()) {
            sb.append(",");
            sb.append(newline);
            sb.append("\"ExpirationTime\": ");
            sb.append(expirationTime.getValue());
            sb.append(newline);
        }
        sb.append("\"OriginColumnToGet\": ");
        sb.append(originColumnsToGet);
        sb.append("}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EnableStream: ");
        sb.append(enableStream);
        if (expirationTime.isValueSet()) {
            sb.append(", ExpirationTime: ");
            sb.append(expirationTime.getValue());
        }
        sb.append(", OriginColumnToGet: ");
        sb.append(originColumnsToGet);
        return sb.toString();
    }
}
