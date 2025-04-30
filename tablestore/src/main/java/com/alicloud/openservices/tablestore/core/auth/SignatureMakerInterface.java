package com.alicloud.openservices.tablestore.core.auth;

import java.io.UnsupportedEncodingException;

import com.alicloud.openservices.tablestore.core.http.RequestMessage;
import com.aliyun.ots.thirdparty.org.apache.http.Header;

public interface SignatureMakerInterface {
    public String getSignature(byte[] accessKey, String action, String method, Header[] headers) throws UnsupportedEncodingException;

    public String getSignatureHeader();

    public void addExtraHeader(RequestMessage request);
}
