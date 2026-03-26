package com.alicloud.openservices.tablestore.core.http;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.JsonError;
import com.google.gson.Gson;

/**
 * Check if there is an error in the returned result.
 * If the returned status code is not 200, throw an <code>OTSException</code> exception.
 */
public class ErrorJsonResponseHandler implements ResponseHandler {
    private static final Gson GSON = new Gson();
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
            throw new ClientException("The endpoint of service has moved to: " + location);
        }

        String requestId = headers.get(Constants.OTS_HEADER_REQUEST_ID);
        if (requestId == null){
            throw new ClientException(
                    "TableStore returns a response with status code: " + responseData.getStatusCode() + ".");
        }

        try (InputStream errorStream = responseData.getContent()) {
            if (errorStream == null) {
                throw new ClientException("Network error.");
            }
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int length;
            int totalBytes = 0;
            final int MAX_ERROR_RESPONSE_SIZE = 64 * 1024; // 64KB limit
            while ((length = errorStream.read(buffer)) != -1) {
                totalBytes += length;
                if (totalBytes > MAX_ERROR_RESPONSE_SIZE) {
                    throw new ClientException("Error response too large, exceeds " + MAX_ERROR_RESPONSE_SIZE + " bytes");
                }
                result.write(buffer, 0, length);
            }
            String responseBody = result.toString(StandardCharsets.UTF_8.name());
            try {
                JsonError jsonError = GSON.fromJson(responseBody, JsonError.class);
                if (jsonError != null && (jsonError.getCode() != null || jsonError.getMessage() != null)) {
                    String message = jsonError.getMessage() != null ? jsonError.getMessage() : "Unknown error";
                    String code = jsonError.getCode() != null ? jsonError.getCode() : "UnknownError";
                    throw new TableStoreException(message, null, code, requestId, httpStatus);
                }
                throw new TableStoreException(responseBody, null, "UnknownError", requestId, httpStatus);
            } catch (com.google.gson.JsonSyntaxException e) {
                throw new TableStoreException(responseBody, null, "UnknownError", requestId, httpStatus);
            }
        } catch (IOException e) {
            throw new ClientException("Failed to read response from server.", e);
        }
    }
}
