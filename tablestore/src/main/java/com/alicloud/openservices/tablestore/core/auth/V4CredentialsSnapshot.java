package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.core.utils.Pair;

public class V4CredentialsSnapshot implements ServiceCredentialsV4 {
    private final String accessKeyId;
    private final String v4SigningAccessKey;
    private final String v4SigningStsToken;
    private final String region;
    private final String signingDate;

    public V4CredentialsSnapshot(
            String accessKeyId,
            String v4SigningAccessKey,
            String v4SigningStsToken,
            String region,
            String signingDate) {
        this.accessKeyId = accessKeyId;
        this.v4SigningAccessKey = v4SigningAccessKey;
        this.v4SigningStsToken = v4SigningStsToken;
        this.region = region;
        this.signingDate = signingDate;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return v4SigningAccessKey;
    }

    public String getSecurityToken() {
        return v4SigningStsToken;
    }

    public String getRegion() {
        return region;
    }

    public String getSigningDate() {
        return signingDate;
    }

    public Pair<String, String> getKeyDatePair() {
        return new Pair<>(v4SigningAccessKey, signingDate);
    }
}