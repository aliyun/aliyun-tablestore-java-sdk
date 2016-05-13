package com.aliyun.openservices.ots.internal;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;
import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.OTS_HEADER_REQUEST_ID;
import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.OTS_HTTP_MOVED_PERMANENTLY;
import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.OTS_MOVED_PERMANENTLY_LOCATION;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.aliyun.openservices.ots.comm.ResponseHandler;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.utils.ServiceConstants;
import com.aliyun.openservices.ots.ClientErrorCode;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.Error;

/**
 * 检查返回结果是否有错误。
 * 如果返回状态码不为200，则抛出<code>OTSException</code>异常。
 *
 */
public class OTSErrorResponseHandler implements ResponseHandler{
    public void handle(ResponseMessage responseData) throws OTSException, ClientException {
        assertParameterNotNull(responseData, "responseData");
        if (responseData.isSuccessful()){
            return;
        }
        
        Map<String, String> headers = responseData.getHeadersMap();
        int httpStatus = responseData.getStatusCode();
       
        if (httpStatus == OTS_HTTP_MOVED_PERMANENTLY) {
            if (!headers.containsKey(OTS_MOVED_PERMANENTLY_LOCATION)){
                throw OTSExceptionFactory.createResponseException(
                        OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_MOVED_PERMANENTLY_LOCATION), null);
            }
            String location = headers.get(OTS_MOVED_PERMANENTLY_LOCATION);
            throw new ClientException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MovedPermanently", location));
        }

        InputStream errorStream = null;
        try {
            errorStream = responseData.getContent();
        } catch (IOException e) {
            throw new ClientException("Failed to read response from server.", e);
        }
        if (errorStream == null){
            throw OTSExceptionFactory.createResponseException(
                    ResourceManager.getInstance(ServiceConstants.RESOURCE_NAME_COMMON)
                        .getString("ServerReturnsUnknownError"),
                    null);
        }
        
        if (!headers.containsKey(OTS_HEADER_REQUEST_ID)){
            // SDK接到OTS服务器返回的异常，但是该异常中未包含协议定义的必选header。
            // 此种情况发生在请求被OTS的HTTP服务器直接拦截，未到OTS Server端被处理。
            throw new ClientException(ClientErrorCode.INVALID_RESPONSE,
                    "OTS returns a response with status code: " + responseData.getStatusCode() + ".", null);
        }

        try {
            Error errMsg = Error.parseFrom(errorStream);
            throw OTSExceptionFactory.create(errMsg, headers.get(OTS_HEADER_REQUEST_ID), httpStatus);
        } catch (IOException e) {
            throw OTSExceptionFactory.createResponseException(
                    ResourceManager.getInstance(ServiceConstants.RESOURCE_NAME_COMMON)
                        .getString("ServerReturnsUnknownError"),
                    e);
        }
    }
}