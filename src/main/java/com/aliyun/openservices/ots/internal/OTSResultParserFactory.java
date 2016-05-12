/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.parser.ResultParser;
import com.google.protobuf.Message;

public class OTSResultParserFactory {

    private OTSResultParserFactory(){
    }

    public static OTSResultParserFactory createFactory() {
        return new OTSResultParserFactory();
    }
    
    public ResultParser createProtocolBufferResultParser(Message m, String traceId) {
        return new ProtocolBufferParser(m, traceId);
    }
}
