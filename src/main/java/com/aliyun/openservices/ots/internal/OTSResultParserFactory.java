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
