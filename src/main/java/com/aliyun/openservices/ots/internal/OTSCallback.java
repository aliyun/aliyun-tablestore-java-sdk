/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.model.OTSContext;

public interface OTSCallback<Req, Res> {
    /**
     * 当请求成功完成时执行
     * @param otsContext
     */
    void onCompleted(OTSContext<Req, Res> otsContext);
    
    /**
     * 在请求失败，且异常为OTSException时执行。
     * @param otsContext
     * @param ex
     */
    void onFailed(OTSContext<Req, Res> otsContext, OTSException ex);
    
    /**
     * 在请求成功，且异常为ClientException时执行。
     * @param otsContext
     * @param ex
     */
    void onFailed(OTSContext<Req, Res> otsContext, ClientException ex);
    
}
