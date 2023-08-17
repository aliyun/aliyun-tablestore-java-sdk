package com.alicloud.openservices.tablestore.core.auth;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.http.RequestMessage;
import org.apache.http.Header;

public class V4SignatureMaker implements SignatureMakerInterface {

    private final ServiceCredentialsV4 credentials;

    public V4SignatureMaker(ServiceCredentialsV4 credentials) {
        this.credentials = credentials;
    }

    public String getSignature(byte[] accessKey, String action, String method, Header[] headers) throws UnsupportedEncodingException {
        StringBuilder canonicalizedOtsHeader = new StringBuilder(1000);
        Map<String, String> headerMap = new TreeMap<String, String>();
        for (Header header : headers) {
            headerMap.put(header.getName(), header.getValue());
        }
        for (Map.Entry<String, String> entry : headerMap.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue().trim();
            if (key.startsWith(Constants.OTS_HEADER_PREFIX)) {
                canonicalizedOtsHeader.append(key).append(":").append(val).append("\n");
            }
        }

        StringBuilder sb = new StringBuilder(1000);
        sb.append("/").append(action).append("\n").append(method).append("\n").
                append("\n").
                append(canonicalizedOtsHeader);
        // add salt for v4 signature
        sb.append(Constants.V4_SIGNATURE_SALT);
        // Hmac SHA-256 for V4 signature
        ServiceSignature signer = new HmacSHA256Signature(accessKey);
        signer.updateUTF8String(sb.toString());

        return signer.computeSignature();
    }

    public String getSignatureHeader() {
        return Constants.OTS_HEADER_SIGNATURE_V4;
    }

    public void addExtraHeader(RequestMessage request) {
        request.addHeader(Constants.OTS_HEADER_ACCESS_KEY_ID, credentials.getAccessKeyId());
        if ((credentials.getSecurityToken() != null) && !credentials.getSecurityToken().isEmpty()) {
            request.addHeader(Constants.OTS_HEADER_STS_TOKEN, credentials.getSecurityToken());
        }

        request.addHeader(Constants.OTS_HEADER_SIGN_DATE, ((V4Credentials) credentials).getSigningDate());
        request.addHeader(Constants.OTS_HEADER_SIGN_REGION, ((V4Credentials) credentials).getRegion());
    }
}
