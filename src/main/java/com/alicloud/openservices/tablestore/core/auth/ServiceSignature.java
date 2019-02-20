package com.alicloud.openservices.tablestore.core.auth;

/**
 * 表示用于计算访问签名的接口。
 */
public interface ServiceSignature {

    public String getAlgorithm();

    public void updateUTF8String(String data);

    public void update(byte[] data);

    public void update(byte data);

    public String computeSignature();
}
