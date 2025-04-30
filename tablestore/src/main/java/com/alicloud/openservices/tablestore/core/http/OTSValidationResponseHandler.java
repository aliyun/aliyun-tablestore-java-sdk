/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 */

package com.alicloud.openservices.tablestore.core.http;

import java.util.Map;
import java.util.TreeMap;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.auth.HmacSHA1Signature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alicloud.openservices.tablestore.core.Constants.*;

/**
 * Verify the return result.
 *
 */
public class OTSValidationResponseHandler implements ResponseHandler{
    private static final Logger LOG = LoggerFactory.getLogger(OTSValidationResponseHandler.class);
    private ServiceCredentials credentials;
    private OTSUri uri;

    public OTSValidationResponseHandler(ServiceCredentials credentials, OTSUri uri){
        Preconditions.checkNotNull(credentials);
        Preconditions.checkNotNull(uri);
        this.credentials = credentials;
        this.uri = uri;
    }

    public void handle(ResponseMessage responseData) throws ClientException {
        Map<String, String> headers = responseData.getLowerCaseHeadersMap();

        // Verify the integrity of the header information
        if (!headers.containsKey(OTS_HEADER_OTS_CONTENT_MD5)) {
            throw new ClientException("MissingHeader: " + OTS_HEADER_OTS_CONTENT_MD5);
        }
        if (!headers.containsKey(OTS_HEADER_OTS_CONTENT_TYPE)) {
            throw new ClientException("MissingHeader: " + OTS_HEADER_OTS_CONTENT_TYPE);
        }
        if (!headers.containsKey(OTS_HEADER_AUTHORIZATION)) {
            throw new ClientException("MissingHeader: " + OTS_HEADER_AUTHORIZATION);
        }

        // Verify authorization information
        StringBuilder strToSign = new StringBuilder(1000);
        Map<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(headers);
        for(Map.Entry<String, String> entry : sortedMap.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            if (key.startsWith(OTS_HEADER_PREFIX)){
                strToSign.append(key);
                strToSign.append(':');
                strToSign.append(val);
                strToSign.append('\n');
            }
        }
        strToSign.append('/');
        strToSign.append(uri.getAction());
        HmacSHA1Signature signer = new HmacSHA1Signature(Bytes.toBytes(credentials.getAccessKeySecret()));
        signer.updateUTF8String(strToSign.toString());
        String actualSign = signer.computeSignature();

        String authHeader = headers.get(OTS_HEADER_AUTHORIZATION);

        int posSign = authHeader.indexOf(actualSign);
        if (posSign < 0) {
            // cannot find signature
            LOG.error("Validate response authorization failed, cannot find signature. headers:{}, accessKeyId:{}, computedSign:{}", headers, credentials.getAccessKeyId(), actualSign);
            throw new ClientException("Validate response authorization failed, cannot find signature.");
        }
        if (posSign == 0 || authHeader.charAt(posSign - 1) != ':') {
            // cannot find separator ':'
            LOG.error("Validate response authorization failed, cannot find separator ':'. headers:{}, accessKeyId:{}, computedSign:{}", headers, credentials.getAccessKeyId(), actualSign);
            throw new ClientException("Validate response authorization failed, cannot find separator ':'.");
        }
        if (posSign + actualSign.length() != authHeader.length()) {
            // signature is not the last part of authHeader
            LOG.error("Validate response authorization failed, signature is not the last part of authHeader. headers:{}, accessKeyId:{}, computedSign:{}", headers, credentials.getAccessKeyId(), actualSign);
            throw new ClientException("Validate response authorization failed, signature is not the last part of authHeader.");
        }
    }
}
