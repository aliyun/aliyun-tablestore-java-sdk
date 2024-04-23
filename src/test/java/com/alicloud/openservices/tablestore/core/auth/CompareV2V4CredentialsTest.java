package com.alicloud.openservices.tablestore.core.auth;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.alicloud.openservices.tablestore.core.http.RequestMessage;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
import org.apache.http.client.methods.HttpPost;
import org.junit.Before;
import org.junit.Test;

public class CompareV2V4CredentialsTest {

    private List<String> accessKeyIDList = new ArrayList<String>();
    private List<String> accessKeySecretList = new ArrayList<String>();
    private int repeatTimes = 1000000;
    private Random random = new Random();

    @Before
    public void prepare() {
        for (int i = 0; i < repeatTimes; i++) {
            accessKeyIDList.add(getRandomString(24));
            accessKeySecretList.add(getRandomString(30));
        }
    }

    /**
     * 测试对比v2签名和v4签名的性能
     */
    @Test
    public void testV2V4CredentialsPerformance() throws UnsupportedEncodingException, InterruptedException {
        Thread.sleep(1000);
        {
            long v4TestStartTime = System.currentTimeMillis();
            for (int i = 0; i < repeatTimes; i++) {
                HttpPost httpPost = new HttpPost("testURI");
                RequestMessage request = new RequestMessage(httpPost);
                ServiceCredentials credentials = new DefaultCredentials(accessKeyIDList.get(i), accessKeySecretList.get(i));
                ServiceCredentials v4Credentials = V4Credentials.createByServiceCredentials(credentials, "cn-test");
                SignatureMakerInterface signatureMaker = SignatureMakerFactory.getSignatureMaker(v4Credentials);
                signatureMaker.addExtraHeader(request);
                signatureMaker.getSignatureHeader();
                signatureMaker.getSignature(Bytes.toBytes(accessKeySecretList.get(i)), "testAction", "POST", request.getRequest().getAllHeaders());
            }
            long v4TestEndTime = System.currentTimeMillis();
            System.out.println("使用V2签名生成V4签名，并使用V4签名共" + repeatTimes + "次总用时：" + (v4TestEndTime - v4TestStartTime));
        }
        Thread.sleep(1000);
        {
            long v4TestStartTime = System.currentTimeMillis();
            for (int i = 0; i < repeatTimes; i++) {
                HttpPost httpPost = new HttpPost("testURI");
                RequestMessage request = new RequestMessage(httpPost);
                ServiceCredentialsV4 v4Credentials = new V4Credentials(accessKeyIDList.get(i), accessKeySecretList.get(i), "cn-test", "20230520");
                SignatureMakerInterface signatureMaker = SignatureMakerFactory.getSignatureMaker(v4Credentials);
                signatureMaker.addExtraHeader(request);
                signatureMaker.getSignatureHeader();
                signatureMaker.getSignature(Bytes.toBytes(accessKeySecretList.get(i)), "testAction", "POST", request.getRequest().getAllHeaders());
            }
            long v4TestEndTime = System.currentTimeMillis();
            System.out.println("使用派生秘钥构造V4签名，并使用V4签名共" + repeatTimes + "次总用时：" + (v4TestEndTime - v4TestStartTime));
        }
        Thread.sleep(1000);
        {
            long v2TestStartTime = System.currentTimeMillis();
            for (int i = 0; i < repeatTimes; i++) {
                HttpPost httpPost = new HttpPost("testURI");
                RequestMessage request = new RequestMessage(httpPost);
                ServiceCredentials credentials = new DefaultCredentials(accessKeyIDList.get(i), accessKeySecretList.get(i));
                SignatureMakerInterface signatureMaker = SignatureMakerFactory.getSignatureMaker(credentials);
                signatureMaker.addExtraHeader(request);
                signatureMaker.getSignatureHeader();
                signatureMaker.getSignature(Bytes.toBytes(accessKeySecretList.get(i)), "testAction", "POST", request.getRequest().getAllHeaders());
            }
            long v2TestEndTime = System.currentTimeMillis();
            System.out.println("使用V2签名共" + repeatTimes + "次总用时：" + (v2TestEndTime - v2TestStartTime));
        }
    }

    public String getRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
