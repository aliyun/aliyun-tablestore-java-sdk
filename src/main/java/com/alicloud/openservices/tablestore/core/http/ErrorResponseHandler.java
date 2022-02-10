package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 检查返回结果是否有错误。
 * 如果返回状态码不为200，则抛出<code>OTSException</code>异常。
 */
public class ErrorResponseHandler implements ResponseHandler {
    public void handle(ResponseMessage responseData) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(responseData);
        if (responseData.isSuccessful()){
            return;
        }
        
        Map<String, String> headers = responseData.getLowerCaseHeadersMap();
        int httpStatus = responseData.getStatusCode();
       
        if (httpStatus == Constants.OTS_HTTP_MOVED_PERMANENTLY) {
            if (!headers.containsKey(Constants.OTS_MOVED_PERMANENTLY_LOCATION)){
                throw new ClientException("The required header is missing: " + Constants.OTS_MOVED_PERMANENTLY_LOCATION);
            }
            String location = headers.get(Constants.OTS_MOVED_PERMANENTLY_LOCATION);
            throw new ClientException("The endpoint of service has moved to:" + location);
        }

        InputStream errorStream = null;
        try {
            errorStream = responseData.getContent();
        } catch (IOException e) {
            throw new ClientException("Failed to read response from server.", e);
        }

        if (errorStream == null){
            throw new ClientException("Network error.");
        }

        String requestId = headers.get(Constants.OTS_HEADER_REQUEST_ID);
        if (requestId == null){
            // SDK接到OTS服务器返回的异常，但是该异常中未包含协议定义的必选header。
            // 此种情况发生在请求被OTS的HTTP服务器直接拦截，未到OTS Server端被处理。
            throw new ClientException(
                    "TableStore returns a response with status code: " + responseData.getStatusCode() + ".");
        }

        try {
            OtsInternalApi.Error errMsg = OtsInternalApi.Error.parseFrom(errorStream);
            throw new TableStoreException(errMsg.getMessage(), null, errMsg.getCode(), requestId, httpStatus);
        } catch (IOException e) {
            throw new ClientException("Network error.", e);
        }
    }
}
