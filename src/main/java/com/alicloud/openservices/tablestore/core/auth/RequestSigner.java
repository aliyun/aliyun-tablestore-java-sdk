package com.alicloud.openservices.tablestore.core.auth;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.http.RequestMessage;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.apache.http.Header;

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
        try {
            SignatureMakerInterface signatureMaker = SignatureMakerFactory.getSignatureMaker(credentials);
            signatureMaker.addExtraHeader(request);
            String signature = signatureMaker.getSignature(
                    accessKey,
                    request.getActionUri().getAction(),
                    request.getRequest().getMethod(),
                    request.getRequest().getAllHeaders());
            request.addHeader(signatureMaker.getSignatureHeader(), signature);
        } catch (UnsupportedEncodingException e) {
            throw new ClientException("无法计算签名：" + e.getMessage());
        }
    }
}
