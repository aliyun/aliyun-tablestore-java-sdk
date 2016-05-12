/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.auth;

import com.aliyun.openservices.ots.comm.RequestMessage;
import com.aliyun.openservices.ots.ClientException;

public interface RequestSigner {

    public void sign(RequestMessage request)
            throws ClientException;
}
