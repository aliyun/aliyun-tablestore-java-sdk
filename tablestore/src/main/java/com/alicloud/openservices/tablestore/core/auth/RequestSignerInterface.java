package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.http.RequestMessage;

public interface RequestSignerInterface {

    public void sign(RequestMessage request)
            throws ClientException;
}
