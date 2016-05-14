package com.aliyun.openservices.ots.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.openservices.ots.utils.DateUtil;
import com.aliyun.openservices.ots.utils.HttpUtil;
import com.aliyun.openservices.ots.utils.ServiceConstants;

public class ServiceSignatureTest {
    /**
     * Test method for {@link com.aliyun.common.auth.ServiceSignature#computeSignature(java.lang.String, java.lang.String)}.
     * @throws Exception 
     */
    @Test
    public void testComputeSignature() throws Exception {
        ServiceSignature sign = ServiceSignature.create();
        String key = "csdev";
        String data = "/ListTable\n"
                + "Date=Mon%2C%2028%20Nov%202011%2014%3A02%3A46%20GMT"
                + "&OTSAccessKeyId=csdev&APIVersion=1"
                + "&SignatureMethod=HmacSHA1&SignatureVersion=1";
        String expected = "3mAoxRc8hd7WdaFB5Ii7dGNcwx0=";

        String signature = sign.computeSignature(key, data);
        assertEquals(expected, signature);

        Map<String, String> parameters = new LinkedHashMap<String, String>();
        Date dt;
        try {
            dt = DateUtil.parseRfc822Date("Mon, 28 Nov 2011 14:02:46 GMT");
            parameters.put("Date", DateUtil.formatRfc822Date(dt));
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (UnsupportedOperationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
        parameters.put("OTSAccessKeyId", "csdev");
        parameters.put("APIVersion", "1");
        parameters.put("SignatureMethod", "HmacSHA1");
        parameters.put("SignatureVersion", "1");
        data = "/ListTable\n" + HttpUtil.paramToQueryString(parameters, ServiceConstants.DEFAULT_ENCODING); //OTSUtil.buildRequestParamString(parameters);
        signature = sign.computeSignature("csdev", data);
        assertEquals(expected, signature);
    }
}
