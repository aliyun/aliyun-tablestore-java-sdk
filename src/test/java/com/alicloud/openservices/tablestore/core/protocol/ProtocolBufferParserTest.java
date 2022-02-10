package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.http.ResponseMessage;
import com.alicloud.openservices.tablestore.core.utils.HttpResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
public class ProtocolBufferParserTest {
    @Test
    public void testGetLargeObject() throws ResultParseException {
        StringBuilder largeStringBuilder = new StringBuilder();
        int len = 128 * 1024 * 1024;
        for (int i = 0; i < len; i++) {
            largeStringBuilder.append('a');
        }
        String largeMessage = largeStringBuilder.toString();
        Message.Builder builder = OtsInternalApi.Error.newBuilder().setCode(largeMessage).setMessage(largeMessage);
        byte[] serialized = builder.build().toByteString().toByteArray();
        try {
            OtsInternalApi.Error.newBuilder().mergeFrom(new ByteArrayInputStream(serialized)).build();
            fail();
        } catch (InvalidProtocolBufferException ex) {
            // expect
        } catch (IOException e) {
            fail();
        }
        ProtocolBufferParser parser = new ProtocolBufferParser(OtsInternalApi.Error.getDefaultInstance(), "trace_id");
        BasicHttpResponse basicHttpResponse = new BasicHttpResponse(
                new BasicStatusLine(new ProtocolVersion("http", 1,0), 200, ""));
        basicHttpResponse.setHeader("x-ots-requestid", "aaa");
        BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContent(new ByteArrayInputStream(serialized));
        basicHttpResponse.setEntity(entity);
        ResponseMessage responseMessage = new ResponseMessage(basicHttpResponse);

        OtsInternalApi.Error errorMessage = (OtsInternalApi.Error) ((ResponseContentWithMeta)parser.getObject(responseMessage)).getMessage();
        assertEquals(largeMessage, errorMessage.getMessage());
        assertEquals(largeMessage, errorMessage.getCode());
    }
}