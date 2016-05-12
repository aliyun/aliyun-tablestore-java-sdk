/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.model.OTSResult;
import com.aliyun.openservices.ots.parser.ResultParseException;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.utils.ServiceConstants;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.OTS_HEADER_REQUEST_ID;

public class ProtocolBufferParser implements ResultParser {

    private static Logger logger = LoggerFactory.getLogger(ProtocolBufferParser.class);

    private Message message;

    private String traceId;

    public ProtocolBufferParser(Message message, String traceId) {
        this.message = message;
        this.traceId = traceId;
    }

    @Override
    public Object getObject(ResponseMessage response) throws ResultParseException {
        
        Map<String, String> headers = response.getHeadersMap();

        String requestId = headers.get(OTS_HEADER_REQUEST_ID);
        if (requestId == null){
            throw new ClientException("The required header is missing: " + OTS_HEADER_REQUEST_ID);
        }

        try {
            Message result = message.newBuilderForType().mergeFrom(response.getContent()).buildPartial();
            if (!result.isInitialized()) {
                throw new UninitializedMessageException(
                        result).asInvalidProtocolBufferException();
            }

            if (logger.isDebugEnabled()) {
                logger.debug("PBResponseMessage: {}, RequestId: {}, TraceId: {}", result.toString(), requestId, traceId);
            }

            return new ResponseContentWithMeta(
                        result, 
                        new OTSResult(headers.get(OTS_HEADER_REQUEST_ID)));
        } catch(Exception e) {
            throw new ResultParseException(
                    ResourceManager.getInstance(ServiceConstants.RESOURCE_NAME_COMMON)
                    .getString("FailedToParseResponse"), e);
        }
    }

}
