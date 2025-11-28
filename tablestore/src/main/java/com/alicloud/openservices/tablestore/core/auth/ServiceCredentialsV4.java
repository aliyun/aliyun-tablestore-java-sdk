package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.core.utils.Pair;

public interface ServiceCredentialsV4 extends ServiceCredentials {

    public String getRegion();

    public String getSigningDate();

    public Pair<String, String> getKeyDatePair();
}
