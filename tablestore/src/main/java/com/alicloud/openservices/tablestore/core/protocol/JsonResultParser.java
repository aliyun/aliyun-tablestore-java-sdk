package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.http.ResponseMessage;
import com.alicloud.openservices.tablestore.core.utils.GsonUtils;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.memory.AddMemoriesResponse;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonResultParser<T extends Response> implements ResultParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonResultParser.class);
    private static final Gson gson = GsonUtils.getGson();

    private Class<T> jsonClass;
    private String traceId;

    public JsonResultParser(Class<T> jsonClass, String traceId) {
        this.jsonClass = jsonClass;
        this.traceId = traceId;
    }

    @Override
    public Object getObject(ResponseMessage response) throws ResultParseException {

        Map<String, String> headers = response.getLowerCaseHeadersMap();

        String requestId = headers.get(Constants.OTS_HEADER_REQUEST_ID);
        if (requestId == null) {
            throw new ClientException("The required header is missing: " + Constants.OTS_HEADER_REQUEST_ID);
        }

        try {
            T result;

            try (Reader reader = new InputStreamReader(response.getContent(), StandardCharsets.UTF_8)) {
                result = gson.fromJson(reader, jsonClass);
            }

            if (result == null) {
                throw new JsonSyntaxException("Parsed result is null");
            }

            if (logger.isDebugEnabled()) {
                logger.debug("JSON Response: {}, RequestId: {}, TraceId: {}", result.toString(), requestId, traceId);
            }
            if (result instanceof AddMemoriesResponse) {
                AddMemoriesResponse addMemoriesResponse = (AddMemoriesResponse) result;
                addMemoriesResponse.setMemoryRequestId(addMemoriesResponse.getRequestId());
            }
            result.setRequestId(requestId);
            if (traceId != null) {
                result.setTraceId(traceId);
            }
            return result;
        } catch (JsonSyntaxException e) {
            throw new ResultParseException("Failed to parse response as JSON due to syntax error.", e);
        } catch (IOException e) {
            throw new ResultParseException("Failed to read response content.", e);
        } catch (Exception e) {
            throw new ResultParseException("Failed to parse response as JSON.", e);
        }
    }

    public Class<T> getJsonClass() {
        return jsonClass;
    }

    public String getTraceId() {
        return traceId;
    }
}
