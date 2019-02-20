package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.http.RequestMessage;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.apache.http.Header;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

public class RequestSigner implements RequestSignerInterface {

    private ServiceCredentials credentials;
    private byte[] accessKey;
    private byte[] instanceName;

    public RequestSigner(String instanceName, ServiceCredentials credentials) {
        Preconditions.checkNotNull(instanceName);
        Preconditions.checkNotNull(credentials);

        this.credentials = credentials;
        this.accessKey = Bytes.toBytes(credentials.getAccessKeySecret());
        try {
            this.instanceName = instanceName.getBytes(Constants.HTTP_HEADER_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new ClientException("Unsupport encoding: " + Constants.HTTP_HEADER_ENCODING);
        }
    }

    @Override
    public void sign(RequestMessage request) throws ClientException {
        request.addHeader(Constants.OTS_HEADER_ACCESS_KEY_ID, credentials.getAccessKeyId());
        if ((credentials.getSecurityToken() != null) && !credentials.getSecurityToken().isEmpty()) {
            request.addHeader(Constants.OTS_HEADER_STS_TOKEN, credentials.getSecurityToken());
        }
        try {
            String signature = getSignature(
                request.getActionUri().getAction(),
                request.getRequest().getMethod(),
                request.getRequest().getAllHeaders());
            request.addHeader(Constants.OTS_HEADER_SIGNATURE, signature);
        } catch (UnsupportedEncodingException e) {
            throw new ClientException("无法计算签名：" + e.getMessage());
        }
    }

    private String getSignature(String action, String method, Header[] headers)
        throws UnsupportedEncodingException
    {
        StringBuilder canonicalizedOtsHeader = new StringBuilder(1000);
        Map<String, String> headerMap = new TreeMap<String, String>();
        for (Header header : headers) {
            headerMap.put(header.getName(), header.getValue());
        }
        for(Map.Entry<String, String> entry : headerMap.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue().trim();
            if (key.startsWith(Constants.OTS_HEADER_PREFIX)) {
                canonicalizedOtsHeader.append(key).append(":").append(val).append("\n");
            }
        }

        StringBuilder sb = new StringBuilder(1000);
        sb.append("/").append(action).append("\n").append(method).append("\n").
            append("\n").
            append(canonicalizedOtsHeader.toString());

        ServiceSignature signer = new HmacSHA1Signature(accessKey);

        signer.updateUTF8String(sb.toString());

        return signer.computeSignature();
    }
}
