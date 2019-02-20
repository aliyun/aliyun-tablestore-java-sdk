package com.alicloud.openservices.tablestore.core.protocol;

import com.google.protobuf.Message;

public class ResultParserFactory {

    private ResultParserFactory(){
    }

    public static ResultParserFactory createFactory() {
        return new ResultParserFactory();
    }
    
    public ResultParser createProtocolBufferResultParser(Message m, String traceId) {
        return new ProtocolBufferParser(m, traceId);
    }
}
