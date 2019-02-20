package com.alicloud.openservices.tablestore.core.auth;

import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.core.utils.HttpUtil;
import com.alicloud.openservices.tablestore.core.utils.DateUtil;
import org.junit.Test;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestServiceSigner {
    @Test
    public void testComputeSignature() throws Exception {
        String key = "csdev";
        String data = "/ListTable\n"
                + "Date=Mon%2C%2028%20Nov%202011%2014%3A02%3A46%20GMT"
                + "&OTSAccessKeyId=csdev&APIVersion=1"
                + "&SignatureMethod=HmacSHA1&SignatureVersion=1";
        String expected = "3mAoxRc8hd7WdaFB5Ii7dGNcwx0=";

        ServiceSignature sign = new HmacSHA1Signature(Bytes.toBytes(key));
        sign.updateUTF8String("/ListTable\n");
        sign.updateUTF8String("Date=Mon%2C%2028%20Nov%202011%2014%3A02%3A46%20GMT");
        sign.updateUTF8String("&OTSAccessKeyId=csdev&APIVersion=1");
        sign.updateUTF8String("&SignatureMethod=HmacSHA1&SignatureVersion=1");

        String signature = sign.computeSignature();
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
        data = "/ListTable\n" + HttpUtil.paramToQueryString(parameters, Constants.UTF8_ENCODING); //OTSUtil.buildRequestParamString(parameters);

        sign = new HmacSHA1Signature(Bytes.toBytes(key));
        sign.updateUTF8String(data);
        signature = sign.computeSignature();
        assertEquals(expected, signature);
    }
}
