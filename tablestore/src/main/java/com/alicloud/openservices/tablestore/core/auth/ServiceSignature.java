package com.alicloud.openservices.tablestore.core.auth;

/**
 * Represents the interface for calculating access signatures.
 */
public interface ServiceSignature {

    public String getAlgorithm();

    public void updateUTF8String(String data);

    public void update(byte[] data);

    public void update(byte data);

    public String computeSignature();
}
