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
     * 是否开启Stream
     */
    private boolean enableStream = false;

    /**
     * 当开启Stream时，该参数用于设置数据过期时间，单位为小时。
     */
    private OptionalValue<Integer> expirationTime = new OptionalValue<Integer>("ExpirationTime");

    /**
     * 设置Stream数据中原始列列表
     */
    private Set<String> originColumnsToGet = new HashSet<String>();


    /**
     * 构造一个StreamSpecification对象。
     * 注意：传入的enableStream必须为false，原因是当enableStream为true时，必须指定expirationTime。
     *      如果需要设置开启Stream，请使用另一构造函数。
     * @param enableStream 必须为false，代表关闭Stream
     */
    public StreamSpecification(boolean enableStream) {
        if (enableStream) {
            throw new ClientException("Expiration time is required when enableStream is true.");
        }
        setEnableStream(enableStream);
    }

    /**
     * 构造一个StreamSpecification对象。
     * 注意：传入的enableStream必须为true，原因是当enableStream为false时，不能指定expirationTime。
     *      如果需要设置关闭Stream，请使用另一构造函数。
     * @param enableStream 必须为true，代表开启Stream
     * @param expirationTime 单位为小时，必须大于0
     */
    public StreamSpecification(boolean enableStream, int expirationTime) {
        if (!enableStream) {
            throw new ClientException("Expiration time cannot be set when enableStream is false.");
        }
        setEnableStream(enableStream);
        setExpirationTime(expirationTime);
    }

    /**
     * 返回是否开启Stream
     *
     * @return
     */
    public boolean isEnableStream() {
        return enableStream;
    }

    /**
     * 设置是否开启Stream
     *
     * @param enableStream
     */
    public void setEnableStream(boolean enableStream) {
        this.enableStream = enableStream;
    }

    /**
     * 获取expirationTime参数，单位为小时
     *
     * @return expirationTime，若返回－1，代表该值未设置。
     */
    public int getExpirationTime() {
        if (expirationTime.isValueSet()) {
            return expirationTime.getValue();
        } else {
            return -1;
        }
    }

    /**
     * 设置expirationTime参数，单位为小时
     *
     * @param expirationTime
     */
    public void setExpirationTime(int expirationTime) {
        Preconditions.checkArgument(expirationTime > 0, "The expiration time must be greater than 0.");
        this.expirationTime.setValue(expirationTime);
    }

    /**
     * 返回要读取的原始列的名称列表（只读）。
     *
     * @return 原始列的名称的列表（只读）。
     */
    public Set<String> getOriginColumnsToGet() {
        return Collections.unmodifiableSet(originColumnsToGet);
    }

    /**
     * 添加要读取的原始列。
     *
     * @param originColumnName 要返回原始列的名称。
     */
    public void addOriginColumnsToGet(String originColumnName) {
        Preconditions.checkArgument(originColumnName != null && !originColumnName.isEmpty(), "OriginColumn's name should not be null or empty.");
        this.originColumnsToGet.add(originColumnName);
    }

    /**
     * 添加要读取的原始列。
     *
     * @param originColumnNames 要返回原始列的名称。
     */
    public void addOriginColumnsToGet(String[] originColumnNames) {
        Preconditions.checkNotNull(originColumnNames, "originColumnNames should not be null.");
        for (int i = 0; i < originColumnNames.length; ++i) {
            addOriginColumnsToGet(originColumnNames[i]);
        }
    }

    /**
     * 添加要读取的原始列。
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
