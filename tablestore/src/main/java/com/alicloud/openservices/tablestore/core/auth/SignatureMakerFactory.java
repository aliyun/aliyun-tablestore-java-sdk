package com.alicloud.openservices.tablestore.core.auth;

public class SignatureMakerFactory {

    public static SignatureMakerInterface getSignatureMaker(Object credentials) {
        if (credentials instanceof ServiceCredentialsV4) {
            return new V4SignatureMaker((ServiceCredentialsV4)credentials);
        } else {
            return new V2SignatureMaker((ServiceCredentials)credentials);
        }
    }
}
