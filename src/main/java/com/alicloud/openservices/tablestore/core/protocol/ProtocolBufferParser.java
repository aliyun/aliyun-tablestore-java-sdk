package com.alicloud.openservices.tablestore.core.protocol;

import java.util.Map;

import com.google.protobuf.CodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.http.ResponseMessage;

public class ProtocolBufferParser implements ResultParser {

    private static Logger logger = LoggerFactory.getLogger(ProtocolBufferParser.class);
    private static int pbSizeLimit = 1024 * 1024 * 1024; // 1GB

    private Message message;

    private String traceId;

    public ProtocolBufferParser(Message message, String traceId) {
        this.message = message;
        this.traceId = traceId;
    }

    @Override
    public Object getObject(ResponseMessage response)
            throws ResultParseException {
        
        Map<String, String> headers = response.getLowerCaseHeadersMap();

        String requestId = headers.get(Constants.OTS_HEADER_REQUEST_ID);
        if (requestId == null){
            throw new ClientException("The required header is missing: " + Constants.OTS_HEADER_REQUEST_ID);
        }
        
        try {
            CodedInputStream codedInputStream = CodedInputStream.newInstance(response.getContent());
            codedInputStream.setSizeLimit(pbSizeLimit);
            Message.Builder resultBuilder = message.newBuilderForType().mergeFrom(codedInputStream);
            codedInputStream.checkLastTagWas(0);
            Message result = resultBuilder.buildPartial();
            if (!result.isInitialized()) {
                throw new UninitializedMessageException(
                        result).asInvalidProtocolBufferException();
            }

            if (logger.isDebugEnabled()) {
                logger.debug("PBResponseMessage: {}, RequestId: {}, TraceId: {}", result.toString(), requestId, traceId);
            }

            return new ResponseContentWithMeta(
                        result, 
                        new Response(requestId));
        } catch(Exception e) {
            throw new ResultParseException("Failed to parse response as protocol buffer message.", e);
        }
    }

}
