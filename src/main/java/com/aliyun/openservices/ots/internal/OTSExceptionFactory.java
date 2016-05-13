package com.aliyun.openservices.ots.internal;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

import com.aliyun.openservices.ots.ClientErrorCode;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.Error;

public class OTSExceptionFactory{
    
    public static OTSException create(String message, Throwable cause){
        return new OTSException(message, cause);
    }
    
    public static OTSException create(String message, Throwable cause,
            String errorCode, String requestId, int httpStatus){
        return new OTSException(message, cause, errorCode, requestId, httpStatus);
    }

    public static ClientException createResponseException(String message, Throwable cause){
        return new ClientException(
                ClientErrorCode.INVALID_RESPONSE,
                OTSUtil.OTS_RESOURCE_MANAGER.getString("ResponseInvalid") + message,
                cause);
    }

    public static OTSException create(Error errMsg, String requestId, int httpStatus) {
        assertParameterNotNull(errMsg, "errMsg");
        return new OTSException(errMsg.getMessage(), null, errMsg.getCode(), requestId, httpStatus);
    }
}
