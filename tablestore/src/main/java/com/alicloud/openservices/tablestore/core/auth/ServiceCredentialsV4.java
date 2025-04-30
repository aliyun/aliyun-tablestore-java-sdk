package com.alicloud.openservices.tablestore.core.auth;


public interface ServiceCredentialsV4 extends ServiceCredentials {

    public String getRegion();

    public String getSigningDate();
}
