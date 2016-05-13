package com.aliyun.openservices.ots.auth;

import com.aliyun.openservices.ots.comm.RequestMessage;
import com.aliyun.openservices.ots.ClientException;

public interface RequestSigner {

    public void sign(RequestMessage request)
            throws ClientException;
}
