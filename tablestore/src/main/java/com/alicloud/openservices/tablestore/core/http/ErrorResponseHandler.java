package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TableStoreNoPermissionException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Check if there is an error in the returned result.
 * If the returned status code is not 200, throw an <code>OTSException</code> exception.
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
            // The SDK receives an exception returned from the OTS server, but the exception does not include the mandatory header defined in the protocol.
            // This situation occurs when the request is directly intercepted by OTS's HTTP server and is not processed on the OTS Server side.
            throw new ClientException(
                    "TableStore returns a response with status code: " + responseData.getStatusCode() + ".");
        }

        try {
            OtsInternalApi.Error errMsg = OtsInternalApi.Error.parseFrom(errorStream);
            if (errMsg.hasAccessDeniedDetail()) {
                throw new TableStoreNoPermissionException(errMsg.getMessage(), null, errMsg.getCode(), requestId, httpStatus,errMsg.getAccessDeniedDetail());
            }
            throw new TableStoreException(errMsg.getMessage(), null, errMsg.getCode(), requestId, httpStatus);
        } catch (IOException e) {
            throw new ClientException("Network error.", e);
        }
    }
}
